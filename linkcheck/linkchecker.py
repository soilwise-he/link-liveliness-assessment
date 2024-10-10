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
USERAGENT = 'Soilwise Link Liveliness assessment v0.1.0' # Send as user-agent with every request
MAX_WORKERS = 5  # Threads used for url checking

# Load environment variables from .env file
load_dotenv()

STOREINDB = os.environ.get("STOREINDB") or True

# base catalog
base = os.environ.get("OGCAPI_URL") or "https://demo.pycsw.org/gisdata"
collection = os.environ.get("OGCAPI_COLLECTION") or "metadata:main"

# format catalogue path with f-string
catalogue_json_url= f"{base}/collections/{collection}/items?f=json"

class URLChecker:
    def __init__(self, timeout=TIMEOUT):
        self.timeout = timeout
        self.ogc_patterns = {
            'WMS': '/wms',
            'WFS': '/wfs',
            'WCS': '/wcs',
            'CSW': '/csw',
            'WMS': '/ows'
        }

    def process_ogc_url(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
       
        # Check if URL is an OGC service
        service_type = None
        for service, pattern in self.ogc_patterns.items():
            if pattern in parsed_url.path.lower():
                service_type = service
                break
       
        
        if not service_type and 'service' in query_params:
            service_type = query_params['service'][0].upper()
        elif not service_type:
            return url

        # If this is an OGC URL then fire a getcapabilities request and set service type
        # Keep all other existing parameters
        new_params = query_params.copy()
        
        owsparams = "width,height,bbox,version,crs,layers,format,srs,count,typenames,srsName,outputFormat"

        for p in owsparams.split(',')+owsparams.upper().split(','):
            if p in new_params:
                del new_params[p]

        # Add GetCapabilities parameters only if they don't exist
        new_params['request'] = ['GetCapabilities']
        new_params['service'] = [service_type]
        
        # Construct new URL
        new_query = urlencode(new_params, doseq=True)
        # print("New url",parsed_url._replace(query=new_query).geturl())
        return parsed_url._replace(query=new_query).geturl()
    
    def check_url(self, url):
        try:
            # Process url if an ogc service
            processed_url = self.process_ogc_url(url)

            response = requests.head(processed_url, timeout=self.timeout, allow_redirects=True, headers={'User-Agent':USERAGENT})

            print(f'\x1b[36m Success: \x1b[0m {url}')
            return {
                'url': url,
                'status_code': response.status_code,
                'is_redirect': response.url != url,
                'valid': 200 <= response.status_code < 400
            }
        except requests.RequestException as e:
            print(f'\x1b[31;20m Failed: \x1b[0m {url}')
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
    response = requests.get(url, headers={'Accept':'application/json','User-Agent':USERAGENT})
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

def process_item(item, relevant_links):
    if isinstance(item, dict):
        if 'href' in item and item['href'].startswith('http') and not item['href'].startswith(base):
            if item.get('rel','') not in ['self', 'collection']:

                relevant_links.add(item['href'])
    elif isinstance(item, list):
        for element in item:
            process_item(element, relevant_links)

def extract_relevant_links_from_json(json_url):
    try:
        response = requests.get(json_url,headers={'Accept':'application/json','User-Agent':USERAGENT})
        response.raise_for_status()
        data = response.json()
        relevant_links = set()
        for f in data.get('features',{}):
            process_item(f.get('links',[]), relevant_links)
        return relevant_links
    except Exception as e:
        # print(f"Error extracting links from JSON at {json_url}: {e}")
        return set()

def main():
    start_time = time.time()
    if STOREINDB == True:
        conn, cur = setup_database()
    url_checker = URLChecker()

    base_url = base + 'collections/' + collection + '/items?offset='
    total_pages, items_per_page = get_pagination_info(catalogue_json_url)

    # Generate URLs for each page
    print('Extracting links from catalogue...')
    all_relevant_links = set()
   
    # Process catalogue page
    for page in range(total_pages):
        print(f"Processing page {page + 1} of {total_pages} at {time.time()-start_time}")
        for l in extract_relevant_links_from_json(f"{base_url}{page * items_per_page}"):
            if l not in all_relevant_links:
                all_relevant_links.add(l)
        
    print(f"Found {len(all_relevant_links)} unique links to check")
   
    # Check all URLs concurrently
    # print("Checking URLs...")
    results = url_checker.check_urls(all_relevant_links)
   
    # Process results
    if STOREINDB == True:
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
    
    if STOREINDB == True:
        print(f"Total checks performed: {total_checks}")
        print(f"Successful checks: {successful_checks}")

        # Close the database connection
        cur.close()
        conn.close()
if __name__ == "__main__":
    main()
