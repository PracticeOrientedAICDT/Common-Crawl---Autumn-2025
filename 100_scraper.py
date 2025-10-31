import os
import sys
import json
from urllib.parse import urlparse
from Scrape_Utils import extract_test_case, search_and_scrape
from typing import List, Dict, Any

# --- Configuration ---
NUM_TRIALS = 100
OUTPUT_JSON = "scraper_results.json"
# ---------------------

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
        # CURRENTCO indices based on your print output:
        # [0]=Company Number, [1]=URL, [2]=Name, [3]=Postcode, [4]=SIC 1, [5]=SIC 2
        current_co = extract_test_case()
        
        if not current_co or len(current_co) < 6:
            print("  [Warn] Failed to extract test case. Skipping trial.")
            continue

        # 4. Map and clean the ground truth data
        company_number = current_co[0]
        company_name = current_co[2]
        postcode = current_co[3]
        
        # Clean the Ground Truth URL
        ground_truth_url = clean_ground_truth_url(current_co[1])
        if not ground_truth_url:
            print(f"  Skipping trial for '{company_name}' (bad ground truth URL).")
            continue
            
        # Clean SIC codes (remove 'nan')
        sic_codes = [code for code in [current_co[4], current_co[5]] if str(code).lower() != 'nan']

        # Store in the agreed-upon structure
        ground_truth_dict = {
            "company_number": company_number,
            "company_name": company_name,
            "postcode": postcode,
            "sic_codes": sic_codes,
            "ground_truth_url": ground_truth_url
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