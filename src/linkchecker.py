from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qs, urlencode
import subprocess
import psycopg2
import psycopg2.extras
import requests
import math
import time
import re
import os

# When a URL reaches MAX_FAILURES consecutive failures it's marked
# as deprecated and excluded from future insertions in database
MAX_FAILURES = 10

# Load environment variables from .env file
load_dotenv()

# base catalog
base = os.environ.get("OGCAPI_URL") or "https://demo.pycsw.org/gisdata"
collection = os.environ.get("OGCAPI_COLLECTION") or "metadata:main"

# format catalogue path with f-string
catalogue_json_url= f"{base}/collections/{collection}/items?f=json"

def setup_database():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST"),
        port=os.environ.get("POSTGRES_PORT"),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD")
    )
    cur = conn.cursor()
   
    # Drop tables (only for development purposes)
    # cur.execute("DROP TABLE IF EXISTS validation_history CASCADE")
    # cur.execute("DROP TABLE IF EXISTS parent CASCADE")
    # cur.execute("DROP TABLE IF EXISTS links CASCADE")

    # Create or truncate linkchecker_output table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS links (
        id_link SERIAL PRIMARY KEY,
        urlname TEXT UNIQUE,
        status TEXT,
        result TEXT,
        info TEXT,
        warning TEXT,
        deprecated BOOLEAN DEFAULT FALSE,
        consecutive_failures INTEGER DEFAULT 0
    )
    """
    cur.execute(create_table_query)
   
    # Create validation_history table if it doesn't exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS parent (
        id SERIAL PRIMARY KEY,
        parentname TEXT NULL,
        baseref TEXT NULL,
        fk_link INTEGER REFERENCES links(id_link),
        UNIQUE (parentname, baseref, fk_link)
    )
    """)
   
    # Create url_status table if it doesn't exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS validation_history (
        id SERIAL PRIMARY KEY,
        fk_link INTEGER REFERENCES links(id_link),
        validation_result TEXT NOT NULL,
        timestamp TIMESTAMP NOT NULL
    )
    """)
   
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

def extract_links(url):
    try:
        # Skip if URL is an email address
        if url.startswith("mailto:"):
            return []
        # Fetch the HTML content of the webpage
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        html_content = response.text

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find all anchor tags and extract their href attributes
        links = [link.get('href') for link in soup.find_all('a')]

        return links
    except Exception as e:
        print(f"Error extracting links from {url}: {e}")
        return []

def check_single_url(url):
    process = subprocess.Popen([
        "linkchecker",
        "--verbose",
        "--check-extern",
        "--recursion-level=0",
        "--timeout=5",
        "--output=csv",
        url + "?f=html"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Process.communicate is good for shorter-running processes 
    stdout, _ = process.communicate()

    return stdout.decode('utf-8').strip().split('\n')

def run_linkchecker(url):
    # Run LinkChecker Docker command with specified user and group IDs for each URL
    process = subprocess.Popen([
        "linkchecker",
        "--verbose",
        "--check-extern",
        "--recursion-level=1",
        "--timeout=5",
        "--output=csv",
        url + "?f=html"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Process the output line by line and yield each line
    # Memory efficient for large outputs
    for line in process.stdout:
        yield line.decode('utf-8').strip()  # Decode bytes to string and strip newline characters
    # Wait for the process to finish
    process.wait()
       
def insert_or_update_link(conn, urlname, status, result, info, warning, is_valid):
    
    with conn.cursor() as cur:
        # Get current status
        cur.execute("SELECT id_link, consecutive_failures, deprecated FROM links WHERE urlname = %s", (urlname,))
        existing_link = cur.fetchone()    
       
        if existing_link:
            link_id, consecutive_failures, deprecated = existing_link

            if existing_link[2]:
                # Ignore deprecated URL's
                # Deprecated URL's are these urls that consecutive have failed for MAX_FAILURES times
                return None
            
            if not is_valid:
                consecutive_failures += 1
            else:
                consecutive_failures = 0
            
            deprecated = deprecated or (consecutive_failures >= MAX_FAILURES)
        
            # Updade existing link
            cur.execute("""
                UPDATE links SET
                    status = %s,
                    result = %s,
                    info = %s,
                    warning = %s,
                    deprecated = %s,
                    consecutive_failures = %s
                WHERE id_link = %s
            """,(status, result, info, warning, deprecated, consecutive_failures, link_id))
        else:
            # Insert new link (not deprecated on the first insertion)
            cur.execute("""
                INSERT INTO links (urlname, status, result, info, warning, deprecated, consecutive_failures)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id_link
            """, (urlname, status, result, info, warning, False, 0 if is_valid else 1))
        
            link_id = cur.fetchone()[0]
        
        # Insert new record in validation history
        cur.execute("""
            INSERT INTO validation_history(fk_link, validation_result, timestamp)
            VALUES(%s, %s, CURRENT_TIMESTAMP)
        """,(link_id, status))
        conn.commit()
       
        return link_id

def insert_parent(conn, parentname, baseref, link_id):
    with conn.cursor() as cur:
        # Convert empty strings to None
        parentname = parentname if parentname else None
        baseref = baseref if baseref else None

        cur.execute("""
            INSERT INTO parent (parentname, baseref, fk_link)
            VALUES (%s, %s, %s)
            ON CONFLICT (parentname, baseref, fk_link) DO NOTHING
        """, (parentname, baseref, link_id))  
       
        # Commit the transaction
        conn.commit()
           
def is_valid_status(valid_string):
    # Return if status is valid or not
    parts = valid_string.split()
    if parts[0].isdigit():
        if 200 <= int(parts[0]) < 400: # Valid HTTP status codes range
            return True
    return False
   
def get_active_urls(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM validation_history")
        count = cur.fetchone()[0]
       
        if count == 0:
            return None # The table is empty
        else:
            cur.execute("SELECT url FROM validation_history WHERE NOT deprecated")
            return [row[0] for row in cur.fetchall()]
        
def determine_service_type(url):
    ogc_patterns = ['/wms', '/wfs', '/csw', '/wcs', 'service=']
    
    if any(pattern in url.lower() for pattern in ogc_patterns):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        query_params.pop('service', None)
        query_params.pop('request', None)
        
        query_params['request'] = ['GetCapabilities']
        
        if 'service' not in query_params:
            if '/wms' in parsed_url.path.lower():
                query_params['service'] = ['WMS']
            elif '/wfs' in parsed_url.path.lower():
                query_params['service'] = ['WFS']
            elif '/csw' in parsed_url.path.lower():
                query_params['service'] = ['CSW']
            elif '/wcs' in parsed_url.path.lower():
                query_params['service'] = ['WCS']
        
        new_query = urlencode(query_params, doseq=True)
        new_url = parsed_url._replace(query=new_query).geturl()
        
        return new_url
    
    return url

def main():
    start_time = time.time()  # Start timing
    # Set up the database and create the table
    print("Setting PostgreSQL db")
    conn, cur = setup_database()
   
    print('Time started processing links.')
    print(f'Loading {catalogue_json_url} links...')
    total_pages, numbers_returned = get_pagination_info(catalogue_json_url)

    # Base URL
    base_url = base + 'collections/' + collection + '/items?offset='

    # Generate URLs for each page
    urls = [base_url + str(i * numbers_returned) + "&f=html" for i in range(total_pages)]

    # Initialize an empty set to store all unique links
    all_links = set()
    # Iterate through the list of URLs and extract links from each one
    for url in urls:
        extracted_links = extract_links(url)
        all_links.update(extracted_links)  # Add new links to the set of all links
   
    # Specify the fields to include in the CSV file
    fields_to_include = ['urlname', 'parentname', 'baseref', 'valid', 'result', 'warning', 'info']

    print("Checking Links...")
    
    # Run LinkChecker and process the output
    urls_to_recheck = set()
    print("Initial Link Checking...")
    for url in all_links:
        for line in run_linkchecker(url):
            if re.match(r'^http', line):
                values = line.rstrip(';').split(';')
                urlname = values[0]               
                
                # Parse initial check results
                filtered_values = [str(values[i]) if i < len(values) else "" for i in range(len(fields_to_include))]
                urlname, parentname, baseref, valid, result, warning, info = filtered_values
                
                # Determine if URL needs to be rechecked
                processed_url = determine_service_type(urlname)
                if processed_url != urlname:
                    urls_to_recheck.add(processed_url)
                else:
                    # If URL doesn't need reprocessing, insert results directly
                    is_valid = is_valid_status(valid)
                    link_id = insert_or_update_link(conn, urlname, valid, result, info, warning, is_valid)
                    insert_parent(conn, parentname, baseref, link_id)
    
    print("Rechecking OGC processed URLs...")
    for url in urls_to_recheck:
        results = check_single_url(url)
        for line in results:
            if re.match(r'^http', line):
                values = line.rstrip(';').split(';')
                filtered_values = [str(values[i]) if i < len(values) else "" for i in range(len(fields_to_include))]
                urlname, parentname, baseref, valid, result, warning, info = filtered_values
                is_valid = is_valid_status(valid)
                link_id = insert_or_update_link(conn, urlname, valid, result, info, warning, is_valid)
                insert_parent(conn, parentname, baseref, link_id)

    # conn.commit()
    print("LinkChecker output written to PostgreSQL database")

    # Close the connection and cursor
    cur.close()
    conn.close()

    end_time = time.time()
    elapsed_time = end_time - start_time 
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
 
if __name__ == "__main__":
    main()
