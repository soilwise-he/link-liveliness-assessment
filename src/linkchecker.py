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

    # Check if the table exists
    cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'linkchecker_output')")
    table_exists = cur.fetchone()[0]

    if table_exists:
        # If the table exists, truncate it and reset the primary key sequence
        cur.execute("TRUNCATE TABLE linkchecker_output RESTART IDENTITY")
    else:
        # If the table does not exist, create it
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
        
    # Check if the validation_history table exists
    cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'validation_history')")
    validation_history_table_exists = cur.fetchone()[0]
    
    if not validation_history_table_exists:
        # Create the validation_history table if it doesn't exist
        create_validation_history_table = """
            CREATE TABLE validation_history (
                id SERIAL PRIMARY KEY,
                url TEXT NOT NULL,
                validation_result TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        cur.execute(create_validation_history_table)
        
    # Commit the changes
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

def insert_validation_history(conn, url, validation_result):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO validation_history (url, validation_result) VALUES (%s, %s)",
            (url, validation_result)
        )
    conn.commit()

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
            
            insert_validation_history(conn, filtered_values[0], filtered_values[3])
    
    print("LinkChecker output written to PostgreSQL database")

    # Close the connection and cursor
    cur.close()
    conn.close()

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
 
if __name__ == "__main__":
    main()

