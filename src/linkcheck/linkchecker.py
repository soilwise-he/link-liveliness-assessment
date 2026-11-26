import traceback

from dotenv import load_dotenv
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, unquote
from urllib.robotparser import RobotFileParser

from concurrent.futures import ThreadPoolExecutor, as_completed
from ogc_services import process_ogc_links
import psycopg2
import requests
import time
import json
import re
import os

# Configuration constants

# ─── Agent ──────────────────────────────────────────────
USERAGENT = 'Soilwise Link Liveliness assessment v0.1.0'  # Sent with every request

# ─── Timeouts ────────────────
TIMEOUT = 30          # Normal links: max wait for the reply
CONNECT_TIMEOUT = 5   # Connection timeout for normal links
LOOKUP_TIMEOUT = 5    # Lookup services (DOI, Handle, w3id)

# ─── Lookup services ───
LOOKUP_SERVICES = {"doi.org", "dx.doi.org", "hdl.handle.net", "w3id.org"}

# ─── Requests  ─────────────────
MAX_WORKERS = 10           # Threads for checking links; one thread per site
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

def site_host(url):
    """Lowercase hostname without 'www.', used to group links by site."""
    host = (urlparse(url).hostname or "unknown").lower()
    return host[4:] if host.startswith("www.") else host

def is_lookup_service(url):
    return site_host(url) in LOOKUP_SERVICES

def head_then_get(session, url, timeout, allow_redirects):
    """HEAD first; if the server answers with an error, try GET (some servers reject HEAD)."""
    response = session.head(url, timeout=timeout, allow_redirects=allow_redirects)
    if response.status_code >= 400:
        response = session.get(url, timeout=timeout,
                               allow_redirects=allow_redirects, stream=True)
        response.close()   # only headers are needed; frees the connection for reuse
    return response

def get_crawl_delay(session, host):
    if host == "unknown":
        return DEFAULT_CRAWL_DELAY, None, True
    try:
        # Download robots.xt
        response = session.get(f"https://{host}/robots.txt", timeout=LOOKUP_TIMEOUT)
        if response.status_code != 200:
            return DEFAULT_CRAWL_DELAY, None, True
        
        # Read it with RobotFileParser
        rp = RobotFileParser()
        rp.parse(response.text.splitlines())
        delay = rp.crawl_delay(USERAGENT)

        if delay is None:
            return DEFAULT_CRAWL_DELAY, rp, True
        return min(float(delay), MAX_CRAWL_DELAY), rp, True
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        print(f"robots.txt not reachable for {host}: {e}")
        return DEFAULT_CRAWL_DELAY, None, False
    except Exception as e:
        print(f"Could not read robots.txt for {host}: {e}")
        return DEFAULT_CRAWL_DELAY, None, True

def get_pause(retry_after):
    try:
        return min(int(retry_after), MAX_RATE_LIMIT_PAUSE)
    except (TypeError, ValueError):
        return RATE_LIMIT_PAUSE
    
def site_down_result(link):
    """Failed result for a link that wasn't checked because its site is down."""
    return {
        'url': link['url'],
        'record_id': link['record_id'],
        'status_code': None,
        'is_redirect': None,
        'valid': False,
        'http_valid': False,
        'content_type': None,
        'content_size': None,
        'last_modified': None,
        'gis_capabilities': None,
        'caps_fetched': False,
        'error': 'Site unreachable (robots.txt and first link failed), not checked',
    }
    
class URLChecker:
    def __init__(self, timeout=TIMEOUT):
        self.timeout = timeout

    def check_lookup_url(self, session, url):
        # Ask only lookup service, don't follow the redirect
        response = head_then_get(session, url, LOOKUP_TIMEOUT, allow_redirects=False)

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

    def check_url(self, session, url):
        try:
            if is_lookup_service(url):
                return self.check_lookup_url(session, url)

            response = head_then_get(session, url, (CONNECT_TIMEOUT, self.timeout), allow_redirects=True)

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
        with requests.Session() as session:
            session.headers.update({'User-Agent': USERAGENT})
            site["crawl_delay"], rp, reachable = get_crawl_delay(session, host)
            print(f"{host}: crawl_delay={site['crawl_delay']}s, links={len(site['links'])}")
            results = []
            site_down = False
            for i, link in enumerate(site["links"]):
                if rp and not rp.can_fetch(USERAGENT, link["url"]):
                    print(f"Blocked by robots.txt: {link['url']}")
                    results.append({'url': link['url'], 'record_id': link['record_id'], 'robots_blocked': True})
                    continue
                
                if site_down:
                    results.append(site_down_result(link))
                    continue
                
                if i > 0:
                    time.sleep(site["crawl_delay"])
                result = self.check_url(session, link["url"])
                
                #robots.txt didn't answer and the first link didn't either -> site is down
                if not reachable and result['status_code'] is None:
                    print(f"{host} is down, marking remaining links as failed")
                    site_down = True
                    
                retries = 0
                while result.get('status_code') == 429 and retries < MAX_429_RETRIES:
                    pause = get_pause(result.get('retry_after')) # 'Retry-After' header or default pause
                    print(f"429 from {host}, pausing site for {pause}s")
                    time.sleep(pause)
                    result = self.check_url(session, link["url"])
                    retries += 1
                    
                # If still rate-limited after retries log it
                if result.get('status_code') == 429:
                    print(f"Still rate-limited (429), skipping: {link['url']}")
                    results.append({'url': link['url'], 'record_id': link['record_id'], 'rate_limited': True})
                    continue

                result['record_id'] = link['record_id']
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
        """One task per site; yields each site's results as soon as that site is done."""
        # Biggest sites first, so they don't start last and slow down the whole run
        ordered = sorted(sites.items(), key=lambda x: len(x[1]["links"]), reverse=True)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            site_jobs = [executor.submit(self.check_site, host, site) for host, site in ordered]
            for job in as_completed(site_jobs):
                yield job.result()

def setup_database(conn):
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
                """, (record_id,))
                record_result = cur.fetchone()

                if record_result:
                    record_db_id = record_result[0]
                else:
                    cur.execute("SELECT id FROM records WHERE record_id = %s",
                              (record_id,))
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

def log_not_checked(conn, url, message):
    """History row for a link that wasn't really checked (429, robots.txt). No failure counted."""
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO links (urlname) VALUES (%s) ON CONFLICT (urlname) DO NOTHING", (url,))
            cur.execute("SELECT id_link FROM links WHERE urlname = %s", (url,))
            link_id = cur.fetchone()[0]
            cur.execute("INSERT INTO validation_history (fk_link, error_message) VALUES (%s, %s)",
                        (link_id, message))
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error logging {url}: {e}")

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

def build_link_info(url, protocol, name, record):
    """Info dict for one http(s) link incl. its service type; None for other links."""
    if url.startswith('http'):
        service_type = detect_service_type(url, protocol)
        return {
            'record_id': record,
            'name': name,
            'url': url,
            'protocol': protocol,
            'service_type': service_type,
        }

# HTML-escaped '&' ('&amp;'), also when it was percent-encoded afterwards:
# '&amp;', '&amp%3B', '%26amp;', '%26amp%3B', '%2526amp%253B'
ESCAPED_AMP = re.compile(r'(&|%26|%2526)amp(;|%3B|%253B)', re.IGNORECASE)

def clean_url(url):
    """Fix catalogue URLs that were HTML-escaped or percent-encoded as a whole.
    Any other URL is left exactly as it is."""
    url = url.strip()

    # 1. Whole URL encoded ('https%3A%2F%2Fex.org...') → decode it once
    if url.lower().startswith(('http%3a', 'https%3a')):
        url = unquote(url)

    # 2. Whole query encoded ('?service%3DWMS%26request%3D...') → decode the query once
    base, sep, query = url.partition('?')
    if sep and '=' not in query and '%3d' in query.lower():
        url = base + '?' + unquote(query)

    # 3. Escaped '&' → '&' (repeat for double escaping, e.g. '&amp;amp;')
    while ESCAPED_AMP.search(url):
        url = ESCAPED_AMP.sub('&', url)

    return url

def load_sites(conn):
    # Read links from the database
    filter = ""
    if os.environ.get("LL_LIMIT"):
        filter = f" LIMIT {os.environ.get("LL_LIMIT")}"
    with conn.cursor() as cur:
        cur.execute("""SELECT DISTINCT ON (record_id, url) 
                       record_id, url, format, name 
                       FROM metadata.distributions""" + filter)
        rows = cur.fetchall()
    print(f'Extracting {len(rows)}...')

    links = {}
    for record_id, url, protocol, name in rows:
        if not url:
            continue   # no URL (NULL or empty) → nothing to check
        url = clean_url(url)
        info =build_link_info(url, protocol, name, record_id)
        if info:
            links[url] = info   # same URL again → later record wins

    # Capabilities dates from earlier runs (none if the links table doesn't exist yet)
    caps_dates = {}
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('links')")
        if cur.fetchone()[0] is not None:
            cur.execute("SELECT urlname, caps_last_checked FROM links")
            caps_dates = dict(cur.fetchall())

    # Group by site
    caps_cutoff = datetime.now() - timedelta(days=CAPS_REFRESH_DAYS)
    sites = {}
    for url, info in links.items():
        site = sites.setdefault(site_host(url), {"crawl_delay": None, "links": []})
        last_checked = caps_dates.get(url)
        site["links"].append({
            "url": url,
            "record_id": info['record_id'],
            "name": info['name'],
            "service_type": info['service_type'],
            "caps_due": last_checked is None or last_checked < caps_cutoff,
        })
    return sites

def connect_db():
    opts = ''
    if os.environ.get("POSTGRES_SCHEMA"):
        opts = f"-c search_path={os.environ.get('POSTGRES_SCHEMA')}"
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST"),
        port=os.environ.get("POSTGRES_PORT"),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        options=opts
    )
    
def save_results_to_db(conn, results):
    for result in results:
        if result.get('robots_blocked'):
            log_not_checked(conn, result['url'], 'Blocked by robots.txt, not checked')
        elif result.get('rate_limited'):
            log_not_checked(conn, result['url'], 'Rate limited (429), link not marked as broken')
        else:
            insert_or_update_link(conn, result, result['record_id'])
            
def main():
    start_time = time.time()
    con = connect_db()
    try:
        if STOREINDB:
            setup_database(con)
        sites = load_sites(con)
        results = []
        for site_results in URLChecker().check_sites(sites):
            if STOREINDB:
                save_results_to_db(con, site_results)
            results.extend(site_results)
        checked = [r for r in results if not r.get('robots_blocked') and not r.get('rate_limited')]
        print(f"\nTime elapsed: {time.time() - start_time:.2f} seconds")
        print(f"Links checked: {len(checked)}, valid: {sum(r['valid'] for r in checked)}, invalid: {sum(not r['valid'] for r in checked)}")
    finally:
        con.close()
        
if __name__ == "__main__":
    main()