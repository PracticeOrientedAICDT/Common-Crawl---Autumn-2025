import os
import sys
import json
from urllib.parse import urlparse
from Scrape_Utils import extract_test_case_TP,extract_test_case_CH, search_and_scrape
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup
from typing import Optional, List, Dict, Any
import html2text
import re
import requests
import json
import pandas as pd

# --- Configuration ---
NUM_TRIALS = 100
OUTPUT_JSON = "scraper_results_6419_CH.json"
# ---------------------
# THIS IS MY PLACEHOLDER  and Needs to be replaced if we want to breakdown the HTML reduce the size of the prompts etc - CHANGE THIS! 

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

def clean_ground_truth_url(raw_url: str) -> str | None:
    """
    Parses a full URL and returns just the scheme + domain.
    e.g., 'https://www.example.com/privacy' -> 'https://www.example.com'
    """
    try:
        parsed = urlparse(raw_url)
        if not parsed.scheme or not parsed.netloc:
            # Handle cases like 'example.com' (missing scheme)
            if not parsed.scheme and parsed.path.startswith('www.'):
                 parsed = urlparse(f"http://{parsed.path}") # Assume http
            else:
                 print(f"  [Warn] Skipping URL: Invalid structure '{raw_url}'")
                 return None
                 
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception as e:
        print(f"  [Warn] Skipping URL: Error parsing '{raw_url}'. Error: {e}")
        return None

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
    
    # Get the first 3 items from the 'organic' list - if you want to change number, adjust here!
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
        
        # --- Part 3: Scrape and Store Data in Memory --- CALL A DIFFERENT FUNCTION HERE TO REDUCE SIZE USE ALTNERATIVE ( ADVISE using MD on the smaller scraped data still) 
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

def main():
    """
    Main function to run the scraping and data-gathering experiment.
    """
    print(f"Starting scraper... Will run {NUM_TRIALS} trials.")
    
    # 1. Get API Key
    s_api_key = os.environ.get('SERPER_API_KEY')
    if not s_api_key:
        print("Error: SERPER_API_KEY environment variable not set.", file=sys.stderr)
        sys.exit(1)

    all_trials_data: List[Dict[str, Any]] = []

    # 2. Start the main loop
    for i in range(NUM_TRIALS):
        print(f"\n--- [Trial {i+1}/{NUM_TRIALS}] ---")
        
        # 3. Get a random test case
        # CURRENTCO indices
        # The extract test case function returns genereates a random test case from a CSV file. There are two versions CH and TP.
        #CH returns data from the Sanj's CSV of UK companies with combined info. TP returns data from Priya trustpilot CSV.
        current_co = extract_test_case_CH()
        print(f"  Test case data: {current_co}")
        if not current_co or len(current_co) < 5:
            print("  [Warn] Failed to extract test case. Skipping trial.")
            continue

        # 4. Map and clean the ground truth data - You need to make sure this matches your CSV structure!
        company_number = current_co[0]
        company_name = current_co[1]
        postcode = current_co[2]
        
        # Clean the Ground Truth URL - You may or may not need this, depending on whether you are matching URLs later so more inmportant for TrustPilot
        #ground_truth_url = clean_ground_truth_url(current_co[1])
        #if not ground_truth_url:
        #    print(f"  Skipping trial for '{company_name}' (bad ground truth URL).")
        #    continue
            
        # extract sic codes
        sic_codes_desc = current_co[3]
        sic_codes_no = current_co[4]
        # Store in the agreed-upon structure
        ground_truth_dict = {
            "company_number": company_number,
            "company_name": company_name,
            "postcode": postcode,
            "sic_code_desc": sic_codes_desc,
            "sic_code_no": sic_codes_no,
            #"ground_truth_url": ground_truth_url -- You may or may not need this depending on whether you have any Ground Truth do important for Trust piliot 
        }

        # 5. Build the search query (using your confirmed logic)
        search_query = f"{company_name} {postcode} {company_number} company website"
        print(f"  Query: '{search_query}'")
        
        # 6. Run the search and scrape process
        # This function already prints its own progress (searching, scraping, etc.)
        scraped_results = search_and_scrape(search_query, s_api_key)

        # 7. Bundle all data for this trial
        trial_data = {
            "trial_number": i + 1,
            "ground_truth_data": ground_truth_dict,
            "search_query_used": search_query,
            "scraped_results": scraped_results  # This is the list from search_and_scrape
        }
        
        all_trials_data.append(trial_data)
        
        print(f"  Trial {i+1} complete. Found {len(scraped_results)} results.")

    # 8. After the loop, save all data to the JSON file
    print(f"\n--- All {NUM_TRIALS} trials complete. ---")
    try:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(all_trials_data, f, indent=2, ensure_ascii=False)
        print(f"✅ Successfully saved all results to **{OUTPUT_JSON}**")
    except Exception as e:
        print(f"❌ Critical Error: Failed to write JSON file. Error: {e}")

if __name__ == "__main__":
    main()