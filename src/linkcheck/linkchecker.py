import traceback

from dotenv import load_dotenv
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, unquote
from urllib.robotparser import RobotFileParser

from concurrent.futures import ThreadPoolExecutor
from ogc_services import process_ogc_links
import psycopg2
import requests
import time
import json
import os

# Configuration constants

# ─── Agent ──────────────────────────────────────────────
USERAGENT = 'Soilwise Link Liveliness assessment v0.1.0'  # Sent with every request

# ─── Timeouts ────────────────
TIMEOUT = 30          # Normal links
LOOKUP_TIMEOUT = 5    # Lookup services (DOI, Handle, w3id)

# ─── Lookup services ───
LOOKUP_SERVICES = {"doi.org", "dx.doi.org", "hdl.handle.net", "w3id.org"}

# ─── Requests  ─────────────────
MAX_WORKERS = 5           # Threads for checking links; one thread per site
DEFAULT_CRAWL_DELAY = 1   # Seconds between requests to one site if robots.txt has no Crawl-delay
MAX_CRAWL_DELAY = 10      # Override robots.txt Crawl delay if it says more than 10 seconds

# ─── Rate limiting ( 429 Too Many Requests ) ─────────────
RATE_LIMIT_PAUSE = 30       # Pause if the site sends no Retry-After header
MAX_RATE_LIMIT_PAUSE = 60   # Never pause longer than this
MAX_429_RETRIES = 1         # Retries after a 429 before giving up on that link

# ─── Link status ───────────────────────────────────────────
MAX_FAILURES = 10        # Failed checks in a row before a URL is marked deprecated
CAPS_REFRESH_DAYS = 30   # Refresh ogc capabiltiies every 30 days

# Load environment variables from .env file
load_dotenv()

FULL_REFRESH = os.environ.get("FULL_REFRESH", "").lower() == "true"
STOREINDB = os.environ.get("STOREINDB", "true").lower() == "true"

# base catalog
base = os.environ.get("OGCAPI_URL") or "https://demo.pycsw.org/gisdata"
collection = os.environ.get("OGCAPI_COLLECTION") or "metadata:main"

# format catalogue path with f-string
catalogue_domain= f"{base}/collections/{collection}/items/"

def is_lookup_service(url):
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host in LOOKUP_SERVICES

def get_crawl_delay(host):
    if host == "unknown":
        return DEFAULT_CRAWL_DELAY
    try:
        # Download robots.xt
        response = requests.get(f"https://{host}/robots.txt",
                                timeout=LOOKUP_TIMEOUT,
                                headers={'User-Agent': USERAGENT})
        if response.status_code != 200:
            return DEFAULT_CRAWL_DELAY

        # Read it with RobotFileParser
        rp = RobotFileParser()
        rp.parse(response.text.splitlines())
        delay = rp.crawl_delay(USERAGENT)

        if delay is None:
            return DEFAULT_CRAWL_DELAY
        return min(float(delay), MAX_CRAWL_DELAY)
    except Exception as e:
        print(f"Could not read robots.txt for {host}: {e}")
        return DEFAULT_CRAWL_DELAY

def get_pause(retry_after):
    try:
        return min(int(retry_after), MAX_RATE_LIMIT_PAUSE)
    except (TypeError, ValueError):
        return RATE_LIMIT_PAUSE

class URLChecker:
    def __init__(self, timeout=TIMEOUT):
        self.timeout = timeout

    def check_lookup_url(self, url):
        # Ask only lookup service, don't follow the redirect
        response = requests.head(url, timeout=LOOKUP_TIMEOUT,
                                 allow_redirects=False,
                                 headers={'User-Agent': USERAGENT})
        # Get request if HEAD is not allowed for LOOKUP_SERVICES
        if response.status_code == 405:
            response = requests.get(url, timeout=LOOKUP_TIMEOUT,
                                    allow_redirects=False, stream=True,
                                    headers={'User-Agent': USERAGENT})
        status = response.status_code
        return {
            'url': url,
            'status_code': status,
            'is_redirect': 300 <= status < 400,
            'valid': 200 <= status < 400,  # redirect = OK, 404/410 = broken
            'content_type': None,
            'content_size': None,
            'last_modified': None,
            'gis_capabilities': None,
            'retry_after': response.headers.get('Retry-After')
        }

    def check_url(self, url):
        try:
            if is_lookup_service(url):
                return self.check_lookup_url(url)

            response = requests.head(url, timeout=self.timeout,
                                    allow_redirects=True,
                                    headers={'User-Agent': USERAGENT})

            # If head request fails, try GET request
            if response.status_code >= 400:
                response = requests.get(url, timeout=self.timeout,
                                       allow_redirects=True, stream=True,
                                       headers={'User-Agent': USERAGENT})

            # Get content type from header
            content_type = response.headers.get('content-type','').split(';')[0]
            last_modified = response.headers.get('last-modified')

            # Get content size from header
            content_size = None
            if 'content-length' in response. headers:
                content_size = int(response.headers['content-length'])
            elif 'content-range' in response.headers:
                range_header = response.headers['content-range']
                if 'bytes' in range_header and '/' in range_header:
                    content_size = int(range_header.split('/')[-1])

            return {
                'url': url,
                'status_code': response.status_code,
                'is_redirect': response.url != url,
                'valid': 200 <= response.status_code < 400,
                'content_type': content_type,
                'content_size': content_size,
                'last_modified': last_modified,
                'gis_capabilities': None,
                'retry_after': response.headers.get('Retry-After')
            }
        except Exception as e:
            print(f'\x1b[31;20m Failed: \x1b[0m {url}; Error: {str(e)}')
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

    def check_site(self, host, site):
        """Check one site's links one by one, waiting crawl_delay seconds between requests."""
        results = []
        for i, link in enumerate(site["links"]):
            if i > 0:
                time.sleep(site["crawl_delay"])
            result = self.check_url(link["url"])
            retries = 0
            while result.get('status_code') == 429 and retries < MAX_429_RETRIES:
                pause = get_pause(result.get('retry_after')) # 'Retry-After' header or default pause
                print(f"429 from {host}, pausing site for {pause}s")
                time.sleep(pause)
                result = self.check_url(link["url"])
                retries += 1
            # If still rate-limited after retries log it
            if result.get('status_code') == 429:
                result['rate_limited'] = True
                results.append(result)
                continue

            result['http_valid'] = result['valid']  # HTTP result before any override
            result['gis_capabilities'] = None
            result['caps_fetched'] = False # Set to True if GetCapabilities was fetched

            # GetCapabilities: monthly refresh, or second opinion when HEAD/GET failed
            if link["service_type"] and (link["caps_due"] or not result['valid']):
                time.sleep(site["crawl_delay"])  # wait before fetching capabilities
                caps = process_ogc_links(link["url"], link["service_type"],
                                         link["name"], link["record_id"])
                result['gis_capabilities'] = caps
                result['caps_fetched'] = True
                if caps and not result['valid']:
                    result['valid'] = True
                    result['error'] = 'HEAD/GET failed, but GetCapabilities succeeded'

            results.append(result)

        return results

    def check_sites(self, sites):
        """One task per site; a free thread takes the next waiting site."""
        # Biggest sites first, so they don't start last and slow down the whole run
        ordered = sorted(sites.items(), key=lambda x: len(x[1]["links"]), reverse=True)
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            site_jobs = [executor.submit(self.check_site, host, site) for host, site in ordered]
            for job in site_jobs:
                results.extend(job.result())
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
    # if os.environ.get("POSTGRES_SCHEMA"): # else it will drop the records table from public
    #     cur.execute("DROP TABLE IF EXISTS records CASCADE")
    # cur.execute("DROP TABLE IF EXISTS links CASCADE")
    # cur.execute("DROP TABLE IF EXISTS validation_history CASCADE")

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
            gis_capabilities JSONB DEFAULT '{}'::JSONB,
            caps_last_checked TIMESTAMP
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

    # Create indexes for better performance
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_validation_latest ON validation_history (fk_link, timestamp DESC)"
    ]

    for index in indexes:
        cur.execute(index)

    if FULL_REFRESH:
        cur.execute("TRUNCATE links, validation_history CASCADE")
    conn.commit()
    return conn, cur

def safe_serialize(obj):
    # Try objects that expose their attributes
    if hasattr(obj, "__dict__"):
        return obj.__dict__

    # Fall back to a string representation
    return str(obj)

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

            gis_caps = json.dumps(url_result['gis_capabilities'], default=safe_serialize, indent=2) if url_result['gis_capabilities'] else '{}'

            cur.execute("""
                INSERT INTO links (urlname, fk_record, consecutive_failures, link_type, link_size, last_modified, gis_capabilities, caps_last_checked)
                VALUES (%s, %s, %s, %s, %s, %s, %s, CASE WHEN %s THEN NOW() END)
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
                    link_type = CASE WHEN %s THEN EXCLUDED.link_type ELSE links.link_type END,
                    link_size = CASE WHEN %s THEN EXCLUDED.link_size ELSE links.link_size END,
                    last_modified = CASE WHEN %s THEN EXCLUDED.last_modified ELSE links.last_modified END,
                    gis_capabilities = CASE WHEN %s THEN EXCLUDED.gis_capabilities ELSE links.gis_capabilities END,
                    caps_last_checked = CASE WHEN %s THEN NOW() ELSE links.caps_last_checked END
                RETURNING id_link, deprecated
            """, (
                    urlname,
                    record_db_id,
                    0 if url_result['valid'] else 1,
                    url_result['content_type'],
                    url_result['content_size'],
                    url_result['last_modified'],
                    gis_caps,
                    url_result.get('caps_fetched') and bool(url_result['gis_capabilities']),   # caps_last_checked (insert)
                    url_result['valid'], # reset failure counter to 0; otw add 1
                    url_result['valid'], # deprecated = false
                    MAX_FAILURES,
                    url_result['http_valid'], # link type
                    url_result['http_valid'], # link size
                    url_result['http_valid'], # last modified
                    url_result.get('caps_fetched') and bool(url_result['gis_capabilities']),   # gis_capabilities
                    url_result.get('caps_fetched') and bool(url_result['gis_capabilities']),   # caps_last_checked
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
        print(f"Error processing URL {url_result['url']}: {str(e)}\nStack trace:\n{traceback.format_exc()}")
        return None

def log_rate_limited(conn, url_result):
    """Record a 429 in validation_history without touching the links table
    (no failure counted, old values kept). Only for links already in the DB."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id_link, deprecated FROM links WHERE urlname = %s",
                        (url_result['url'],))
            row = cur.fetchone()
            if row is None or row[1]:
                return  # new link (nothing to attach to) or deprecated link
            cur.execute("""
                INSERT INTO validation_history(
                    fk_link, status_code,
                    is_redirect, error_message
                )
                VALUES(%s, %s, %s, %s)
            """, (
                row[0],
                url_result['status_code'],
                url_result['is_redirect'],
                'Rate limited (429), link not marked as broken'
            ))
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error logging rate limit for {url_result['url']}: {str(e)}")

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

def process_url(url, protocol, name, record):
    """Process URL and utilize process_ogc_api to get capabilities.

    Args:
        url (str): URL to process
        record (str): Record ID to use as metadata ID for OGC services

    Returns:
        object: A object containing the URL and its capabilities
    """
    if url.startswith('http'):
        service_type = detect_service_type(url, protocol)
        return {
            'record_id': record,
            'name': name,
            'url': url,
            'protocol': protocol,
            'service_type': service_type,
        }

def main():
    start_time = time.time()
    if STOREINDB:
        conn, cur = setup_database()
    url_checker = URLChecker()

    cur.execute("""
            SELECT
                record_id, url, format, name
            FROM
                metadata.distributions
        """)
    records = cur.fetchall()

    print(f'Extracting {len(records)}...')
    url_record_map = {}  # Dictionary to store URL to record_id mapping
    cur.execute("SELECT urlname, caps_last_checked FROM links")
    caps_dates = dict(cur.fetchall())  # {url: date or None}
    for r in records:
        print(f"Processing link {unquote(r[1]).split('?')[0]} from record {r[0]} at {time.time()-start_time}")
        pre = process_url(unquote(r[1]), r[2], r[3], r[0])
        if pre:
            url_record_map[unquote(r[1])] = pre

    caps_cutoff = datetime.now() - timedelta(days=CAPS_REFRESH_DAYS)
    sites = {}
    # Group by hostname
    for url, info in url_record_map.items():
        host = (urlparse(url).hostname or "unknown").lower()
        if host.startswith("www."):
            host = host[4:]
        if host not in sites:
            sites[host] = {"crawl_delay": None, "links": []}
        last_checked = caps_dates.get(url)
        sites[host]["links"].append({
            "url": url,
            "record_id": info['record_id'],
            "name": info['name'],
            "service_type": info['service_type'],
            "caps_due": last_checked is None or last_checked < caps_cutoff
        })

    # Read each site's crawl-delay from robots.txt
    for host in sites:
        sites[host]["crawl_delay"] = get_crawl_delay(host)
        print(f"{host}: crawl_delay={sites[host]['crawl_delay']}s, links={len(sites[host]['links'])}")

    # Check links: one thread per site, crawl-delay between requests
    results = url_checker.check_sites(sites)

    # Process results
    if STOREINDB:
        processed_links = 0
        for result in results:
            if result.get('rate_limited'):
                print(f"Skipping {result['url']}: still rate-limited (429)")
                log_rate_limited(conn, result)
                continue
            # Get both record_id and capabilities from the map
            record_info = url_record_map[result.get('url','')]

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
