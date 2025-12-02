import requests
from bs4 import BeautifulSoup
from typing import Optional, List, Dict, Any
import html2text
import csv
from time import sleep
import re
import requests
import json
from urllib.parse import urlparse
import pandas as pd
import random
import jellyfish


def should_recurse(embedded_url: str, original_url: str, company_name: str) -> bool:
    """
    Decide if an embedded link is worth recursing on.
    """
    # Don't recurse to same domain
    if urlparse(embedded_url).netloc == urlparse(original_url).netloc:
        return False
    
    # Don't recurse to known aggregators
    aggregator_domains = ['endole.co.uk', 'companieshouse.gov.uk', 'scooploop.com']
    if any(agg in embedded_url for agg in aggregator_domains):
        return False
    
    # Optional: String similarity check on embedded URL domain
    domain_fragment = get_domain_fragment(embedded_url)
    if URL_similarity_match(company_name, domain_fragment):
        return True  # High confidence, definitely recurse
    
    # For others, maybe recurse with lower priority
    return True

#scrape to markdown function
def ScrapeToMarkdown(url: str) -> Optional[str]:
    """
    Fetches the content of a given URL, cleans the HTML, and converts 
    the body content into a Markdown-formatted string suitable for an LLM.

    Args:
        url: The URL of the webpage to scrape.

    Returns:
        A string containing the page content in Markdown format, or None on failure.
    
    Dependancies: beautifulsoup4, html2text, requests
    """
    try:
        # 1. Fetch the raw HTML content
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        # 2. Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # 3. Clean the HTML (Remove noise before conversion)
        # Remove script, style, and footer/nav elements that clutter LLM context
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'form', 'aside']):
            tag.decompose()
        
        # 4. Extract the body content
        body_content = str(soup.body) if soup.body else str(soup)

        # 5. Convert the cleaned HTML to Markdown
        h = html2text.HTML2Text()
        h.ignore_links = False  # Set to True if you want to remove all links
        h.ignore_images = True  # Images are usually noise for text extraction
        h.body_width = 0        # Don't wrap lines (better for LLM tokenizing)
        
        markdown_text = h.handle(body_content)

        # Basic final cleaning to remove excessive whitespace/empty lines
        clean_text = '\n'.join([line.strip() for line in markdown_text.splitlines() if line.strip()])

        return clean_text

    except requests.exceptions.HTTPError as e:
        print(f"Error fetching {url}: HTTP Error - {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: Connection/Request Error - {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

#search and scrape function function

def search_and_scrape(search_query: str, api_key: str) -> List[Dict[str, Any]]:
    """
    Orchestrates the full process:
    1. Searches using Serper API.
    2. Scrapes the TOP 3 results (filtering out .gov.uk).
    3. Returns all data as a list of dictionaries in memory.
    """
    print(f"Starting search for: '{search_query}'...")
    results_json = SerphSearch(search_query, api_key)

    # This list will hold our final data
    scraped_data = []

    # --- Part 1: Handle Search Results ---
    if 'error' in results_json:
        print("Script terminated due to a search error.")
        return scraped_data  # Return the empty list

    # --- Part 2: Handle Scraping Top 3 Results ---
    
    # Get the first 3 items from the 'organic' list
    top_results = results_json.get('organic', [])[:3]

    if not top_results:
        print("No organic search results found to scrape.")
        return scraped_data  # Return the empty list

    print(f"\nFound {len(top_results)} results. Attempting to scrape (will filter '.gov.uk')...")

    # Loop over each of the top results
    for i, result in enumerate(top_results):
        url_to_scrape = result.get('link')
        title = result.get('title', f'result_{i+1}')
        position = result.get('position', i + 1)
        
        print(f"\n--- [Result {position}] Processing: '{title}' ---")

        if not url_to_scrape:
            print("❌ Skipping: No URL found for this result.")
            continue  # Skip to the next result in the loop

        try:
            # We need to parse the URL to check its domain (netloc)
            parsed_url = urlparse(url_to_scrape)
            if parsed_url.netloc.endswith(".gov.uk"):
                print(f"❌ Skipping URL: {url_to_scrape} (domain .gov.uk)")
                continue  # Skip to the next result in the loop
        except Exception as e:
            # Catch any potential URL parsing errors
            print(f"⚠️  Error parsing URL for filter: {e}. Skipping.")
            continue
            
        print(f"URL: {url_to_scrape}")
        
        # --- Part 3: Scrape and Store Data in Memory ---
        markdown_content = ScrapeToMarkdown(url_to_scrape)

        if markdown_content:
            print("✅ Scraping Successful.")

            # Create the dynamic filename string
            cleaned_url = re.sub(r'^https?://', '', url_to_scrape)
            safe_filename_base = re.sub(r'[^a-zA-Z0-9]', '_', cleaned_url)
            filename = f"{position}_{safe_filename_base}.md"
            
            # Append the data to our list
            scraped_data.append({
                "position": position,
                "title": title,
                "link": url_to_scrape,
                "filename": filename,
                "markdown_content": markdown_content
            })
                
        else:
            print("❌ Failed to scrape this webpage.")
            
    # After the loop, return the complete list of scraped data
    print(f"\n--- Process complete. Returning {len(scraped_data)} scraped items. ---")
    return scraped_data

# convert json to csv fuction. 

def convert_json_to_csv(json_data: Dict[str, Any], filename: str):
    """
    Converts the nested search JSON data into a flat CSV file using pandas.
    """
    # Check for the primary list of results, often 'organic' or 'news'
    if 'organic' not in json_data:
        print("Error: JSON data does not contain the 'organic' results list needed for CSV conversion.")
        return

    # Use json_normalize to flatten the list of dictionaries
    # This automatically handles nested fields by creating new columns (e.g., 'title', 'snippet', 'sitelinks.0.title')
    try:
        df = pd.json_normalize(json_data['organic'])
        
        # Clean up column names by replacing dots with underscores
        df.columns = df.columns.str.replace('.', '_', regex=False)

        # Save the DataFrame to a CSV file
        df.to_csv(filename, index=False, encoding='utf-8')
        
        print(f"Successfully saved {len(df)} records to **{filename}**")
        print("Top 5 columns created:", list(df.columns[:5]))
        
    except Exception as e:
        print(f"An error occurred during CSV conversion: {e}")
# Search fuction
def SerphSearch(search_string: str, api_key: str) -> Dict[str, Any]:
    """
    Performs a Google search using the Serper API, returning the structured JSON result. first page of results only. 

    Args:
        search_string: The query string to search for (from the LLM or wherever we get it from) .
        api_key: I don't want to put this on GitHub!

    Returns:
        A dictionary containing the structured JSON search results from the API.
        If an error occurs, returns a dictionary with an "error" key.
    """
    
    url = "https://google.serper.dev/search"
    
    # 1. Prepare the payload with the dynamic search string
    payload = json.dumps({
      "q": search_string,
      "location": "United Kingdom",
      "gl": "gb"
    })
    
    # 2. Prepare the headers, using the input api_key
    headers = {
      'X-API-KEY': api_key,
      'Content-Type': 'application/json'
    }

    # 3. Execute the API request
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status() 
        
        # 4. Return the parsed JSON response as a Python dictionary
        return response.json()
        
    except requests.exceptions.HTTPError as http_err:
        # Handle HTTP errors (e.g., 401 Unauthorized for bad API key, 429 Rate Limit)
        print(f"HTTP error occurred: {http_err} - Response: {response.text}")
        return {"error": f"HTTP Error: {http_err}", "details": response.text}
    except requests.exceptions.RequestException as req_err:
        # Handle other request errors (e.g., connection errors, timeouts)
        print(f"Request error occurred: {req_err}")
        return {"error": f"Request Error: {req_err}"}



def load_companies_from_csv(filepath: str):
    """
    Loads company data from a CSV and normalizes fields.
    """
    print(f"\n📂 Loading Companies House data from {filepath}...")
    companies = []
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader, start=1):
            company = {
                "Company number": row.get("CompanyNumber", "").strip(),
                "Company name": row.get("CompanyName", "").strip(),
                "Address": row.get("RegAddress.AddressLine1", "").strip(),
                "Postcode": row.get("RegAddress.PostCode", "").strip(),
                "Sic codes": [code.strip() for code in row.get("SICCode.SicText_1", "").split(",") if code.strip()]
            }
            companies.append(company)
            if i % 50 == 0:
                print(f"Loaded {i} rows so far...")
    print(f"✅ Finished loading {len(companies)} companies.\n")
    return companies


def _clean_string(text: str) -> str:
    """Helper function to normalize and clean company/URL strings."""
    
    # 1. Convert to lowercase
    text = text.lower()
    
    # 2. Define and remove UK-specific suffixes
    # Note: 'limited liability partnership' must be replaced before 'limited' or 'llp'
    suffixes = [
        'limited liability partnership',
        'limited',
        'ltd',
        'llp',
    ]
    
    for suffix in suffixes:
        text = text.replace(suffix, '')
        
    # 3. Remove all special characters, punctuation, and spaces
    # This regex keeps only alphanumeric characters (a-z, 0-9)
    text = re.sub(r'[^a-z0-9]', '', text)
    
    return text

def URL_similarity_match(registered_name: str, url_fragment: str) -> bool:
    """
    Compares a registered company name to a URL fragment to find a match
    based on a Levenshtein similarity ratio.

    Args:
        registered_name: The official registered company name 
                         (e.g., "Acme & Co. Ltd").
        url_fragment: The core part of a URL, with 'www.' and '.com' 
                      removed (e.g., "acme-co").
        
    Returns:
        True if the similarity ratio is >= 0.9, otherwise False.
    """
    
    # 1. Clean both input strings using the helper function
    cleaned_name = _clean_string(registered_name)
    cleaned_url = _clean_string(url_fragment)
    
    # 2. Handle edge case: If the cleaning results in an empty name,
    #    we cannot make a meaningful comparison.
    if not cleaned_name:
        return False
        
    # 3. Compute the Levenshtein similarity ratio
    # --- FIX: START ---
    
    # First, get the raw Levenshtein distance
    lev_dist = jellyfish.levenshtein_distance(cleaned_name, cleaned_url)
    
    # Next, convert the distance to a 0.0-1.0 similarity ratio
    len_sum = len(cleaned_name) + len(cleaned_url)
    
    if len_sum == 0:
        # Both strings were empty, so they are 100% similar
        similarity_ratio = 1.0
    else:
        # This is the formula we discussed
        similarity_ratio = (len_sum - lev_dist) / len_sum
        
    # --- FIX: END ---
        
    # 4. Define the threshold
    threshold = 0.9
    
    # 5. Return True if ratio is at or above the threshold, else False
    # --- FIX: START ---
    # The check is now correctly comparing the ratio to the threshold
    return similarity_ratio >= threshold


def extract_test_case_TP(row_number: Optional[int] = None) -> List[str]:
    """
    Extracts a specific test case row from the 'ground_truth_dataset.csv' file.

    This function reads 'ground_truth_dataset.csv' located in the same folder,
    assuming the first row (row 1) is the header.


    Args:
        row_number (Optional[int]): The 1-based (spreadsheet-style) row number
            to extract. Row 1 is the header, so data starts at row 2.
            If set to None (default), a random data row will be selected.

    Returns:
        List[str]: A list of strings containing the data from the
            specified columns in the selected row.
        
        Returns an empty list [] if the file is not found, the row
        number is out of bounds, or any other error occurs.
    """
    

    COLUMN_NAMES = [
    'company_number',   # The header for (Col A)
    'CompanyName',  # The header for (Col AG)
    'RegAddress.PostCode',   # The header for (Col F)
    'SICCode.SicText_1',  # The header for index 77
    'SICCode.SicText_2',            # The header for index (Col AR)
    'source_url'           # The header for index (Col AS)
]
    FILENAME = "ground_truth_dataset.csv"
    try:
        # Read the CSV. header=0 means the first row (index 0) is the header.
        df = pd.read_csv(FILENAME, header=0, usecols=COLUMN_NAMES)
        
        # Get the total number of *data* rows
        num_data_rows = len(df)
        
        # If the file is empty (no data rows), return empty list
        if num_data_rows == 0:
            return []
            
        selected_row_index = -1

        if row_number is None:
            # --- Random Row Selection ---
            # Select a random 0-based index from 0 to (num_data_rows - 1)
            selected_row_index = random.randint(0, num_data_rows - 1)
            
        else:
            # --- Specific Row Selection ---
            # User provides 1-based row number.
            # Row 1 is header. Row 2 is the *first* data row (index 0).
            # So, we subtract 2 to get the correct 0-based index.
            selected_row_index = row_number - 2
            
            # Validate the requested index
            if not (0 <= selected_row_index < num_data_rows):
                print(f"Error: Row {row_number} is out of bounds.")
                return []
        
        # Select the data using .iloc[row_index, column_indices]
        selected_data = df.loc[selected_row_index, COLUMN_NAMES]
        
        # Convert all items to string and return as a list
        output_list = [str(item) for item in selected_data]
        
        return output_list

    except FileNotFoundError:
        print(f"Error: File '{FILENAME}' not found.")
        return []
    except IndexError:
        # This might happen if the file has fewer columns than requested
        print("Error: Column index out of bounds. Check CSV structure.")
        return []
    except Exception as e:
        # Catch any other potential errors
        print(f"An unexpected error occurred: {e}")
        return []

def extract_test_case_CH(row_number: Optional[int] = None) -> List[str]:
    """
    Extracts a specific test case row from the 'ground_truth_dataset.csv' file.

    This function reads 'ground_truth_dataset.csv' located in the same folder,
    assuming the first row (row 1) is the header.

    It extracts data from the following 1-based (spreadsheet-style) columns:
    - Column 1
    - Column 6
    - Column 4
    
    
    - Column 31

    Args:
        row_number (Optional[int]): The 1-based (spreadsheet-style) row number
            to extract. Row 1 is the header, so data starts at row 2.
            If set to None (default), a random data row will be selected.

    Returns:
        List[str]: A list of strings containing the data from the
            specified columns in the selected row.
        
        Returns an empty list [] if the file is not found, the row
        number is out of bounds, or any other error occurs.
    """
    
    # These are the 1-based (spreadsheet) column numbers you requested.
    # We subtract 1 to get the 0-based index for pandas.
    #COLUMN_INDICES = [1-1, 33-1,6-1,78-1,31-1,]
    COLUMN_NAMES = [
    'company_number',   # The header for (Col A)
    'company_name',  # The header for (Col AG)
    'registered_office_address.postal_code',   # The header for (Col F)
    'sic_descriptions_str',  # The header for index 77
    'sic_codes'            # The header for index (Col AR)
]
    FILENAME = "ground_truth_dataset.csv"
    
    try:
        # Read the CSV. header=0 means the first row (index 0) is the header.
        df = pd.read_csv(FILENAME, header=0, usecols=COLUMN_NAMES)
        
        # Get the total number of *data* rows
        num_data_rows = len(df)
        
        # If the file is empty (no data rows), return empty list
        if num_data_rows == 0:
            return []
            
        selected_row_index = -1

        if row_number is None:
            # --- Random Row Selection ---
            # Select a random 0-based index from 0 to (num_data_rows - 1)
            selected_row_index = random.randint(0, num_data_rows - 1)
            
        else:
            # --- Specific Row Selection ---
            # User provides 1-based row number.
            # Row 1 is header. Row 2 is the *first* data row (index 0).
            # So, we subtract 2 to get the correct 0-based index.
            selected_row_index = row_number - 2
            
            # Validate the requested index
            if not (0 <= selected_row_index < num_data_rows):
                print(f"Error: Row {row_number} is out of bounds.")
                return []
        
        # Select the data using .iloc[row_index, column_indices]
        selected_data = df.loc[selected_row_index, COLUMN_NAMES]
        
        # Convert all items to string and return as a list
        output_list = [str(item) for item in selected_data]
        
        return output_list

    except FileNotFoundError:
        print(f"Error: File '{FILENAME}' not found.")
        return []
    except IndexError:
        # This might happen if the file has fewer columns than requested
        print("Error: Column index out of bounds. Check CSV structure.")
        return []
    except Exception as e:
        # Catch any other potential errors
        print(f"An unexpected error occurred: {e}")
        return []


