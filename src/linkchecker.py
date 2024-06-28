from bs4 import BeautifulSoup
from dotenv import load_dotenv
import subprocess
import psycopg2
import psycopg2.extras
import requests
import math
import time
import re
import os

# When a URL reaches MAX_FAILURES consecutive failures it's marked
# as deprecated and excluded from future checks
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

    # Create or truncate linkchecker_output table
    cur.execute("DROP TABLE IF EXISTS linkchecker_output")
    create_table_query = """
    CREATE TABLE linkchecker_output (
        id SERIAL PRIMARY KEY,
        urlname TEXT,
        parentname TEXT,
        baseref TEXT,
        valid TEXT,
        result TEXT,
        warning TEXT,
        info TEXT,
        url TEXT,
        name TEXT
    )
    """
    cur.execute(create_table_query)
    
    # Create validation_history table if it doesn't exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS validation_history (
        id SERIAL PRIMARY KEY,
        url TEXT NOT NULL,
        validation_result TEXT NOT NULL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Create url_status table if it doesn't exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS url_status (
        url TEXT PRIMARY KEY,
        consecutive_failures INTEGER DEFAULT 0,
        deprecated BOOLEAN DEFAULT FALSE,
        last_checked TIMESTAMP
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

def run_linkchecker(urls):
    for url in urls:
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
        for line in process.stdout:
            yield line.decode('utf-8').strip()  # Decode bytes to string and strip newline characters
        # Wait for the process to finish
        process.wait()

def insert_validation_history(conn, url, validation_result, is_valid):
    with conn.cursor() as cur:
        # Insert new record in validation_history
        cur.execute("""
            INSERT INTO validation_history (url, validation_result)
            VALUES (%s, %s)
        """, (url, validation_result))

        # Get current status
        cur.execute("SELECT consecutive_failures, deprecated FROM url_status WHERE url = %s", (url,))
        result = cur.fetchone()

        if result:
            consecutive_failures, deprecated = result
            if not is_valid:
                consecutive_failures += 1
            else:
                consecutive_failures = 0

            deprecated = deprecated or (consecutive_failures >= MAX_FAILURES)

            # Update url_status
            cur.execute("""
                UPDATE url_status 
                SET consecutive_failures = %s, 
                    deprecated = %s, 
                    last_checked = CURRENT_TIMESTAMP
                WHERE url = %s
            """, (consecutive_failures, deprecated, url))
        else:
            # Insert new url_status if not exists
            cur.execute("""
                INSERT INTO url_status (url, consecutive_failures, deprecated, last_checked)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            """, (url, 0 if is_valid else 1, False))

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
        
def get_all_urls(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM validation_history")
        count = cur.fetchone()[0]
        
        if count == 0:
            return None # The table is empty
        else:
            cur.execute("SELECT url FROM validation_history")
            return [row[0] for row in cur.fetchall()]

def main():
    start_time = time.time()  # Start timing
    # Set up the database and create the table
    print("Setting PostgreSQL db")
    conn, cur = setup_database()
    
    print("Time started processing links.")
    print("Loading EJP SOIL Catalogue links...")
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
    
    # Define the formats to be removed
    formats_to_remove = [
        'collections/' + collection + '/items?offset',
        '?f=json'
    ]
    
    # Get the list of active (non-deprecated) URLs
    all_known_urls = get_all_urls(conn)

    if all_known_urls is None:
        # First run on empty table, check all links
        links_to_check = all_links
    else:
        # Check all known links plus any new links
        links_to_check = set(all_known_urls) | all_links
            
    # Specify the fields to include in the CSV file
    fields_to_include = ['urlname', 'parentname', 'baseref', 'valid', 'result', 'warning', 'info', 'url', 'name']

    print("Checking Links...")
    # Run LinkChecker and process the output
    for line in run_linkchecker(links_to_check):
        if re.match(r'^http', line):
            # Remove trailing semicolon and split by semicolon
            values = line.rstrip(';').split(';')
            filtered_values = [values[field] if field < len(values) else "" for field in range(len(fields_to_include))]
            
            is_valid = False
            if is_valid_status(filtered_values[3]):
                is_valid = True
            # Insert the data into the PostgreSQL table for each link
            insert_query = """
                INSERT INTO linkchecker_output
                (urlname, parentname, baseref, valid, result, warning, info, url, name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_query, filtered_values)
            conn.commit()
            
            insert_validation_history(conn, filtered_values[0], filtered_values[3], is_valid)
    
    print("LinkChecker output written to PostgreSQL database")

    # Close the connection and cursor
    cur.close()
    conn.close()

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
 
if __name__ == "__main__":
    main()

