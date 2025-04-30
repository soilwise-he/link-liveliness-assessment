from bs4 import BeautifulSoup
from dotenv import load_dotenv
from lxml import html
from urllib.parse import urlparse, parse_qs, urlencode
from concurrent.futures import ThreadPoolExecutor
from ogc_services import process_ogc_links
import psycopg2
import requests
import math
import time
import json
import os

# Configuration constants

MAX_FAILURES = 10 # Used to mark deprecated url's
TIMEOUT = 5  # Url timeout
USERAGENT = 'Soilwise Link Liveliness assessment v0.1.0' # Send as user-agent with every request
MAX_WORKERS = 5  # Threads used for url checking
# MAX_PAGES = 2 # Limit the run to a subset or pages

# Load environment variables from .env file
load_dotenv()

STOREINDB = os.environ.get("STOREINDB") or True

# base catalog
base = os.environ.get("OGCAPI_URL") or "https://demo.pycsw.org/gisdata"
collection = os.environ.get("OGCAPI_COLLECTION") or "metadata:main"

# format catalogue path with f-string
catalogue_json_url= f"{base}/collections/{collection}/items?f=json"
catalogue_domain= f"{base}/collections/{collection}/items/"

class URLChecker:
    def __init__(self, timeout=TIMEOUT):
        self.timeout = timeout

    def check_url(self, url):
        try:
            response = requests.head(url, timeout=self.timeout,
                                    allow_redirects=True,
                                    headers={'User-Agent': USERAGENT})
           
            # If head request fails, try GET request
            if response.status_code >= 400:
                response = requests.get(url, timeout=self.timeout,
                                       allow_redirects=True,
                                       headers={'User-Agent': USERAGENT})
               
            # Get content type from header
            content_type = response.headers.get('content-type','').split(';')[0]
            # print("Content type is ",content_type)
            last_modified = response.headers.get('last-modified')

            # Get content size from header
            content_size = None
            if 'content-length' in response. headers:
                content_size = int(response.headers['content-length'])
            elif 'content-range' in response.headers:
                range_header = response.headers['content-range']
                if 'bytes' in range_header and '/' in range_header:
                    content_size = int(range_header.split('/')[-1])

            # print("Url size is",content_size)
            # print(f'\x1b[36m Success: \x1b[0m {url}')
            return {
                'url': url,
                'status_code': response.status_code,
                'is_redirect': response.url != url,
                'valid': 200 <= response.status_code < 400,
                'content_type': content_type,
                'content_size': content_size,
                'last_modified': last_modified,
                'gis_capabilities': None
            }
        except requests.RequestException as e:
            print(f'\x1b[31;20m Failed: \x1b[0m {url}')
            return {
                'url': url,
                'error': str(e),
                'status_code': None,
                'is_redirect': None,
                'valid': False,
                'content_type': None,
                'content_size': None,
                'last_modified': None,
                'gis_capabilities': None
            }

    def check_urls(self, urls):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(self.check_url, urls))
        return results

def setup_database():

    opts=''
    if os.environ.get("POSTGRES_SCHEMA"):
        opts = f"-c search_path={os.environ.get('POSTGRES_SCHEMA')}"

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST"),
        port=os.environ.get("POSTGRES_PORT"),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        options=opts
    )
    cur = conn.cursor()
   
    # Drop existing tables
    if os.environ.get("POSTGRES_SCHEMA"): # else it will drop the records table from public
        cur.execute("DROP TABLE IF EXISTS records CASCADE")
    cur.execute("DROP TABLE IF EXISTS links CASCADE")
    cur.execute("DROP TABLE IF EXISTS validation_history CASCADE")

    # Create tables
    tables = [
        """
        CREATE TABLE IF NOT EXISTS records (
            id SERIAL PRIMARY KEY,
            record_id TEXT UNIQUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS links (
            id_link SERIAL PRIMARY KEY,
            urlname TEXT UNIQUE,
            link_type TEXT,
            link_size BIGINT,
            last_modified TIMESTAMP,
            fk_record INTEGER REFERENCES records(id),
            deprecated BOOLEAN DEFAULT FALSE,
            consecutive_failures INTEGER DEFAULT 0,
            gis_capabilities JSONB DEFAULT '{}'::JSONB
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
    # if MAX_PAGES and MAX_PAGES < total_pages:
    #     total_pages = MAX_PAGES
    return total_pages, number_returned
  except requests.exceptions.RequestException as e:
    print(f"Error fetching or parsing JSON data from {url}: {e}")
    return None
  except Exception as e:
    print(f"Error calculating total pages from JSON data: {e}")
    return None

def insert_or_update_link(conn, url_result, record_id):
    try:
        with conn.cursor() as cur:
            urlname = url_result['url']
           
            if record_id:
                cur.execute("""
                    INSERT INTO records (record_id)
                    VALUES (%s)
                    ON CONFLICT (record_id) DO NOTHING
                    RETURNING id
                """, (catalogue_domain + record_id,))
                record_result = cur.fetchone()
               
                if record_result:
                    record_db_id = record_result[0]
                else:
                    cur.execute("SELECT id FROM records WHERE record_id = %s",
                              (catalogue_domain + record_id,))
                    record_result = cur.fetchone()
                    record_db_id = record_result[0] if record_result else None
            else:
                record_db_id = None
               
            gis_caps = json.dumps(url_result['gis_capabilities']) if url_result['gis_capabilities'] else '{}'
           
            cur.execute("""
                INSERT INTO links (urlname, fk_record, consecutive_failures, link_type, link_size, last_modified, gis_capabilities)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (urlname) DO UPDATE
                SET consecutive_failures = CASE
                        WHEN %s THEN 0
                        ELSE links.consecutive_failures + 1
                    END,
                    deprecated = CASE
                        WHEN %s THEN false
                        WHEN links.consecutive_failures + 1 >= %s THEN true
                        ELSE links.deprecated
                    END,
                    link_type = EXCLUDED.link_type,
                    link_size = EXCLUDED.link_size,
                    last_modified = EXCLUDED.last_modified,
                    gis_capabilities = EXCLUDED.gis_capabilities
                RETURNING id_link, deprecated
            """, (
                    urlname,
                    record_db_id,
                    0 if url_result['valid'] else 1,
                    url_result['content_type'],
                    url_result['content_size'],
                    url_result['last_modified'],
                    gis_caps,
                    url_result['valid'],
                    url_result['valid'],
                    MAX_FAILURES
                ))
           
            link_id, deprecated = cur.fetchone()

            if not deprecated:
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
    except Exception as e:
        conn.rollback()
        print(f"Database error processing URL {url_result['url']}: {str(e)}")
        return None

def detect_service_type(url, protocol=None):
    if not url:
        return None
    
    url_lower = url.lower()
    
    # Check for OGC API patterns in URL
    is_ogcapi_url = any(pattern in url_lower for pattern in ['/ogc/features', '/ogcapi', '/api/features'])
    
    # First check protocol if provided otherwise check URL
    if protocol:
        protocol_lower = protocol.lower()
        
        # Direct OGC API protocol
        if 'ogc api' in protocol_lower:
            return 'ogcapi'
            
        # Protocol specifies WFS but URL suggests OGC API
        elif 'wfs' in protocol_lower and is_ogcapi_url:
            print(f"URL suggests OGCAPI but protocol says WFS, using 'ogcapi' instead for: {url}")
            return 'ogcapi'
            
        # Standard OGC service types
        elif 'wms' in protocol_lower:
            return 'wms'
        elif 'wmts' in protocol_lower:
            return 'wmts'
        elif 'wfs' in protocol_lower:
            return 'wfs'
        elif 'wcs' in protocol_lower:
            return 'wcs'
        elif 'ows' in protocol_lower:
            return 'wms'  # Default OWS to WMS
    
    if is_ogcapi_url:
        return 'ogcapi'
    
    # Check for service parameter in query string
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    if 'service' in query_params:
        service = query_params['service'][0].lower()
        if service in ['wms', 'wmts', 'wfs', 'wcs']:
            return service
    
    # Check URL path for service indicators
    if '/wms' in url_lower:
        return 'wms'
    elif '/wmts' in url_lower:
        return 'wmts'
    elif '/wfs' in url_lower:
        return 'wfs'
    elif '/wcs' in url_lower:
        return 'wcs'
    
    # No clear indication found
    return None

def process_url(link, relevant_links, record):
    """Process URL and utilize process_ogc_api to get capabilities.
   
    Args:
        url (str): URL to process
        relevant_links (list): List to store processed URLs and their capabilities
        record (str): Record ID to use as metadata ID for OGC services
   
    Returns:
        None: Updates relevant_links list in place with (url, capabilities) tuples
    """
    url = link.get('href','')
    protocol = link.get('protocol','')
    layer_name = link.get('name','')
    rel = link.get('rel','')
    name = link.get('name','')

    if url.startswith('http'):
        if ('thumbnail' in protocol.lower() or 
            'image' in protocol.lower() or 
            rel == 'preview' or 
            name == 'preview'):
            # Store as a non-OGC URL
            relevant_links.append((url, None))
            return
        
        # Use the enhanced detect_service_type function that considers both protocol and URL
        service_type = detect_service_type(url, protocol)
        
        # if service_type:
        #     print(f'Detected service: {service_type} for URL: {url}')
        
        # Process OGC API with record ID as metadata ID
        # Pass the protocol to process_ogc_links for additional context
        capabilities_result = process_ogc_links(url, service_type, layer_name, record)
        
        # Store both original URL and capabilities result
        relevant_links.append((url, capabilities_result))
    else:
        # For non-OGC URLs, store with None capabilities
        relevant_links.append((url, None))

def process_item(item, relevant_links, record):
    if isinstance(item, dict) and 'href' in item and item['href'] not in [None, '', 'null']:
        if item['href'].startswith('http'):
            if 'rel' in item and item['rel'] not in [None,''] and item['rel'].lower() in ['collection', 'self', 'root', 'prev', 'next', 'canonical']:
                None
            else:
                process_url(item, relevant_links, record)

def extract_relevant_links_from_json(json_url):
    try:
        response = requests.get(json_url, headers={'Accept':'application/json','User-Agent':USERAGENT})
        response.raise_for_status()
        data = response.json()
        links_map = {}  # Dictionary to store URL:record_id pairs

        features = data.get('features', [])
        if features:
            for feature in features:
                # Get the record ID for this feature
                record_id = feature.get('id')
                if record_id:
                    feature_links = []  
                    for link in feature.get('links', []):
                        process_item(link, feature_links, record_id)
                    # Store both URL and capabilities
                    for url, capabilities in feature_links:
                        links_map[url] = {
                            'record_id': record_id,
                            'capabilities': capabilities
                        }
        return links_map
    except Exception as e:
        print(f"Error extracting links from JSON at {json_url}: {e}")
        return {}

def main():
    start_time = time.time()
    if STOREINDB:
        conn, cur = setup_database()
    url_checker = URLChecker()
   
    base_url = base + 'collections/' + collection + '/items?offset='
    total_pages, items_per_page = get_pagination_info(catalogue_json_url)
   
    print(f'Extracting links from catalogue (first {total_pages} pages)...')
    url_record_map = {}  # Dictionary to store URL to record_id mapping

    # Process only total_pages number of pages
    for page in range(total_pages):
        print(f"Processing page {page + 1} of {total_pages} at {time.time()-start_time}")
        page_url_map = extract_relevant_links_from_json(f"{base_url}{page * items_per_page}")
        url_record_map.update(page_url_map)
   
    print(f"Found {len(url_record_map)} unique links to check")
   
    # Check all URLs concurrently
    results = url_checker.check_urls(url_record_map.keys())
   
    # Process results
    if STOREINDB:
        # print(f"Update database...")
        processed_links = 0
        for result in results:
            # Get both record_id and capabilities from the map
            record_info = url_record_map[result['url']]
            # Update result with capabilities info
            result['gis_capabilities'] = record_info['capabilities']
           
            if insert_or_update_link(conn, result, record_info['record_id']) is not None:
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