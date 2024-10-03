from bs4 import BeautifulSoup
from dotenv import load_dotenv
from lxml import html
from urllib.parse import urlparse, parse_qs, urlencode
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import psycopg2.extras
import requests
import math
import time
import re
import os

# Configuration constants

MAX_FAILURES = 10 # Used to mark deprecated url's
TIMEOUT = 5  # Url timeout
MAX_WORKERS = 5  # Threads used for url checking

# Load environment variables from .env file
load_dotenv()

# base catalog
base = os.environ.get("OGCAPI_URL") or "https://demo.pycsw.org/gisdata"
collection = os.environ.get("OGCAPI_COLLECTION") or "metadata:main"

# format catalogue path with f-string
catalogue_json_url= f"{base}/collections/{collection}/items?f=json"

class URLChecker:
    def __init__(self, timeout=TIMEOUT):
        self.timeout = timeout

    def check_url(self, url):
        try:
            response = requests.head(url, timeout=self.timeout, allow_redirects=True)
           
            return {
                'url': url,
                'status_code': response.status_code,
                'is_redirect': response.url != url,
                'valid': 200 <= response.status_code < 400
            }
        except requests.RequestException as e:
            return {
                'url': url,
                'error': str(e),
                'status_code': None,
                'is_redirect': None,
                'valid': False
            }

    def check_urls(self, urls):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(self.check_url, urls))
        return results

def setup_database():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST"),
        port=os.environ.get("POSTGRES_PORT"),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD")
    )
    cur = conn.cursor()
   
    # Drop existing tables
    cur.execute("DROP TABLE IF EXISTS links CASCADE")
    cur.execute("DROP TABLE IF EXISTS validation_history CASCADE")

    # Create tables
    tables = [
        """
        CREATE TABLE IF NOT EXISTS links (
            id_link SERIAL PRIMARY KEY,
            urlname TEXT UNIQUE,
            deprecated BOOLEAN DEFAULT FALSE,
            consecutive_failures INTEGER DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS validation_history (
            id SERIAL PRIMARY KEY,
            fk_link INTEGER REFERENCES links(id_link),
            status_code INTEGER,
            is_redirect BOOLEAN,
            error_message TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    ]
   
    for table in tables:
        cur.execute(table)
   
    conn.commit()
    return conn, cur

def get_pagination_info(url):
  try:
    # Fetch catalogue JSON
    response = requests.get(url)
    response.raise_for_status()  # Raise exception for HHTP errors
    data = response.json()

    # Extract relevant fields
    number_matched = data.get('numberMatched', 0)
    number_returned = data.get('numberReturned', 0)

    # Calculate total pages
    total_pages = math.ceil(number_matched / number_returned)
    return total_pages, number_returned
  except requests.exceptions.RequestException as e:
    print(f"Error fetching or parsing JSON data from {url}: {e}")
    return None
  except Exception as e:
    print(f"Error calculating total pages from JSON data: {e}")
    return None
 
def insert_or_update_link(conn, url_result):
    with conn.cursor() as cur:
        urlname = url_result['url']
       
        # Get or create link
        cur.execute("""
            INSERT INTO links (urlname, consecutive_failures)
            VALUES (%s, %s)
            ON CONFLICT (urlname) DO UPDATE
            SET consecutive_failures =
                CASE
                    WHEN %s THEN 0
                    ELSE links.consecutive_failures + 1
                END,
                deprecated =
                    CASE
                        WHEN %s THEN false
                        WHEN links.consecutive_failures + 1 >= %s THEN true
                        ELSE links.deprecated
                    END
            RETURNING id_link, deprecated
        """, (urlname, 0 if url_result['valid'] else 1, url_result['valid'], url_result['valid'], MAX_FAILURES))
       
        link_id, deprecated = cur.fetchone()
       
        if not deprecated:
            # Insert validation history
            cur.execute("""
                INSERT INTO validation_history(
                    fk_link, status_code,
                    is_redirect, error_message
                )
                VALUES(%s, %s, %s, %s)
            """, (
                link_id,
                url_result['status_code'],
                url_result['is_redirect'],
                url_result.get('error')
            ))
       
        conn.commit()
        return link_id if not deprecated else None

def extract_relevant_links_from_json(json_url):
    try:
        response = requests.get(json_url)
        response.raise_for_status()
        data = response.json()
        relevant_links = set()
       
        def process_item(item):
            if isinstance(item, dict):
                if 'href' in item and 'rel' not in item:
                    relevant_links.add(item['href'])
                    # print(f"  - Found direct href link: {item['href']}")
                elif 'href' in item and item.get('rel') not in ['self', 'collection']:
                    relevant_links.add(item['href'])
                    # print(f"  - Found relevant link: {item['href']}")
               
                for value in item.values():
                    process_item(value)
            elif isinstance(item, list):
                for element in item:
                    process_item(element)

        process_item(data)
        return relevant_links
    except Exception as e:
        # print(f"Error extracting links from JSON at {json_url}: {e}")
        return set()

def extract_links(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        tree = html.fromstring(response.content)
        return tree.xpath('//a/@href')
    except Exception as e:
        print(f"Error extracting links from {url}: {e}")
        return []

def main():
    start_time = time.time()
    conn, cur = setup_database()
    url_checker = URLChecker()

    # base_url = 'https://catalogue.ejpsoil.eu/collections/metadata:main/items?offset='
    # catalogue_json_url = 'https://catalogue.ejpsoil.eu/collections/metadata:main/items?f=json'
    base_url = base + 'collections/' + collection + '/items?offset='


    total_pages, items_per_page = get_pagination_info(catalogue_json_url)

    # Generate URLs for each page
    print('Extracting links from catalogue...')
    all_relevant_links = set()
   
    # Process catalogue page
    for page in range(total_pages):
        # page_url = f"{base_url}{page * items_per_page}&f=html"
        print(f"Processing page {page + 1} of {total_pages}")

        extracted_links = extract_links(f"{base_url}{page * items_per_page}&f=html")
       
        for link in extracted_links:
            json_url = f"{link}?f=json" if "?f=json" not in link else link
            relevant_links = extract_relevant_links_from_json(json_url)
            all_relevant_links.update(relevant_links)

    print(f"Found {len(all_relevant_links)} unique links to check")
   
    # Check all URLs concurrently
    # print("Checking URLs...")
    results = url_checker.check_urls(all_relevant_links)
   
    # Process results
    print(f"Update database...")
    processed_links = 0
    for result in results:
        if insert_or_update_link(conn, result) is not None:
            processed_links += 1
   
    cur.execute("""
        SELECT
            COUNT(*) as total_checks,
            SUM(CASE WHEN status_code BETWEEN 200 AND 399 THEN 1 ELSE 0 END) as successful_checks
        FROM validation_history
    """)
    total_checks, successful_checks = cur.fetchone()

    end_time = time.time()
    print("\nSummary:")
    print(f"Time elapsed: {end_time - start_time:.2f} seconds")
    print(f"Total checks performed: {total_checks}")
    print(f"Successful checks: {successful_checks}")

    # Close the database connection
    cur.close()
    conn.close()
if __name__ == "__main__":
    main()
