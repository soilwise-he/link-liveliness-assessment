from bs4 import BeautifulSoup
import subprocess
import requests
import math
import time
import csv
import re
import os

ejp_catalogue_json_url = "https://catalogue.ejpsoil.eu/collections/metadata:main/items?f=json"

def get_pagination_info(url):
  try:
    # Fetch ejpsoil JSON
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
            "docker", "run", "--rm", "-i", "-u", "1000:1000", "ghcr.io/linkchecker/linkchecker:latest", 
            "--verbose", "--check-extern", "--recursion-level=1",  "--output=csv",
            url + "?f=html"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Process the output line by line and yield each line
        for line in process.stdout:
            yield line.decode('utf-8').strip()  # Decode bytes to string and strip newline characters
        # Wait for the process to finish
        process.wait()

def main():
    start_time = time.time()  # Start timing
    print("Time  started processing links.")
    print("Loading EJP SOIL Catalogue links...")
    filename = "soil_catalogue_link.csv"
    total_pages, numbers_returned = get_pagination_info(ejp_catalogue_json_url)

    # Base URL
    base_url = 'https://catalogue.ejpsoil.eu/collections/metadata:main/items?offset='

    # Generate URLs for each page i.e(https://catalogue.ejpsoil.eu/collections/metadata:main/items?offset=0,50...)
    urls = [base_url + str(i * numbers_returned) + "&f=html" for i in range(total_pages)]

    # Initialize an empty set to store all unique links
    all_links = set()
    # Iterate through the list of URLs and extract links from each one
    for url in urls:
        extracted_links = extract_links(url)
        all_links.update(extracted_links)  # Add new links to the set of all links
    
    # Define the formats to be removed
    formats_to_remove = [
        'https://catalogue.ejpsoil.eu/collections/metadata:main/items?offset',
        '?f=json'
    ]

    # Filter out links with the specified formats
    filtered_links = {link for link in all_links if not any(format_to_remove in link for format_to_remove in formats_to_remove)}
    
    # Remove the existing file if it exists
    if os.path.exists(filename):
        os.remove(filename)

    # Specify the fields to include in the CSV file
    fields_to_include = ['urlname', 'parentname', 'baseref', 'valid', 'result', 'warning', 'info', 'url', 'name']

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:

        writer = csv.writer(csvfile)

        # Write the header row with field names
        writer.writerow(fields_to_include)
        print("Checking Links...")
        # Run LinkChecker and process the output
        for line in run_linkchecker(filtered_links):
            if re.match(r'^http', line):
                # Remove trailing semicolon and split by semicolon
                values = line.rstrip(';').split(';')
                filtered_values = [values[field] if field < len(values) else "" for field in range(len(fields_to_include))]

                writer.writerow(filtered_values)
    print("CSV data written to", filename)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Time elapsed: {elapsed_time:.2f} seconds")
 
if __name__ == "__main__":
    main()