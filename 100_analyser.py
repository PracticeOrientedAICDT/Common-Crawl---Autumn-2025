import os
import sys
import json
import re
import pandas as pd
from google import genai 
from urllib.parse import urlparse
from Scrape_Utils import URL_similarity_match
from typing import List, Dict, Any, Tuple
import time  # Keep this for the retry logic

# --- Configuration ---
INPUT_JSON = "scraper_results.json"
OUTPUT_CSV = "analysis_results.csv"
# ---------------------

def init_gemini_client():
    """Initializes and returns the Gemini V1 client."""
    print("Initializing Gemini V1 client (genai.Client)...")
    g_api_key = os.environ.get("GOOGLE_API_KEY")
    if not g_api_key:
        print("Error: GOOGLE_API_KEY environment variable not set.", file=sys.stderr)
        return None
    
    try:
        client = genai.Client() 
        print("✅ Gemini V1 client initialized successfully.")
        return client
    except Exception as e:
        print(f"Error initializing Gemini V1 client. \nError: {e}", file=sys.stderr)
        return None

def clean_base_url(raw_url: str) -> str | None:
    """
    Parses a full URL and returns just the scheme + domain.
    e.g., 'https://www.example.com/privacy' -> 'https://www.example.com'
    """
    try:
        parsed = urlparse(raw_url)
        if not parsed.scheme or not parsed.netloc:
            return None
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return None

def get_domain_fragment(url: str) -> str:
    """
    Extracts the core domain fragment for similarity matching.
    e.g., 'https://www.acme-ltd.co.uk/page' -> 'acme-ltd'
    """
    try:
        netloc = urlparse(url).netloc
        if netloc.startswith("www."):
            netloc = netloc[4:]
        fragment = netloc.split('.')[0]
        return fragment
    except Exception:
        return ""

def create_llm_prompt(company_data: Dict[str, Any], scraped_content: str) -> str:
    """Builds the standardized prompt for the LLM."""
    sic_code_str = ", ".join(company_data['sic_codes']) if company_data['sic_codes'] else "N/A"
    
    return f"""
Your task is to find a company's official website by deciding if the two 
sets of information match.
The following rules regarding company information needs to be 
observed:
1. The company name and the company name in the website 
information must be the same, if available
2. The company number and the company number in the website 
information must be the same, if available
3. The company’s address and the address in the website 
information must be the same, if available
4. The industry relating to the company’s SIC code and the 
industry in the website information must be the same, if 
available
5. The website information should not indicate that the company is 
based outside of the United Kingdom

Does the information from entity 2 match the company information 
from entity 1?
Answer with 'Yes' and the corresponding URL if they match, and 
answer with 'No' if they do not.

Entity 1:
Company name: {company_data['company_name']}
Company number: {company_data['company_number']}
Address post code: {company_data['postcode']}
SIC codes: {sic_code_str}

Entity 2:
{scraped_content[:15000]} 

Answer:
"""

def parse_llm_response(response_text: str) -> Tuple[str, str]:
    """
    Parses the raw LLM text response to extract the 'Yes'/'No'
    and any URL it provided.
    """
    response_text = response_text.strip()
    
    if not response_text:
        return "No", ""
        
    llm_answer = "Yes" if response_text.lower().startswith("yes") else "No"
    url_match = re.search(r'https://?:\/\/[^\s\n]+', response_text)
    llm_url = url_match.group(0) if url_match else ""
    
    return llm_answer, llm_url

def main():
    print(f"Loading data from {INPUT_JSON}...")
    try:
        with open(INPUT_JSON, 'r', encoding='utf-8') as f:
            all_trials_data: List[Dict[str, Any]] = json.load(f)
    except FileNotFoundError:
        print(f"Error: {INPUT_JSON} not found. Did you run scraper.py?", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Could not decode {INPUT_JSON}. File might be corrupt.", file=sys.stderr)
        sys.exit(1)
        
    print(f"Loaded {len(all_trials_data)} trials.")

    llm_client = init_gemini_client()
    if not llm_client:
        sys.exit(1)

    analysis_results: List[Dict[str, Any]] = []

    for trial in all_trials_data:
        company_data = trial['ground_truth_data']
        company_name = company_data['company_name']
        ground_truth_url = company_data['ground_truth_url']
        
        print(f"\n--- Processing Trial {trial['trial_number']}: {company_name} ---")

        if not trial['scraped_results']:
            print("  [Warn] No scraped results for this trial. Skipping.")
            continue
            
        for result in trial['scraped_results']:
            scraped_url = result['link']
            scraped_pos = result['position']
            markdown_content = result['markdown_content']
            
            print(f"  Analysing result {scraped_pos}: {scraped_url}")

            cleaned_scraped_url = clean_base_url(scraped_url)
            is_correct_url = (cleaned_scraped_url == ground_truth_url)

            domain_fragment = get_domain_fragment(scraped_url)
            string_match_result = URL_similarity_match(company_name, domain_fragment)

            # --- Rate Limit Logic ---
            
            llm_prompt = create_llm_prompt(company_data, markdown_content)
            
            MAX_RETRIES = 3 # Allow a few retries just in case
            retries = 0
            llm_answer = "ERROR" # Default to error
            llm_url = ""

            while retries < MAX_RETRIES:
                try:
                    # Attempt the API call
                    response = llm_client.models.generate_content(
                        model='gemini-2.5-flash-lite', 
                        contents=llm_prompt
                    )
                    llm_answer, llm_url = parse_llm_response(response.text)
                    print(f"    - String Match: {string_match_result}")
                    print(f"    - LLM Match: {llm_answer}")
                    
                    # If successful, break the retry loop
                    break 
                    
                except Exception as e:
                    # Check if it's the specific rate limit error
                    if "429 RESOURCE_EXHAUSTED" in str(e):
                        retries += 1
                        wait_time = 60 # Wait a full minute for quota to reset
                        print(f"    [Warn] Hit Rate Limit. Retrying {retries}/{MAX_RETRIES} in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        # It's a different, non-retryable error
                        print(f"    [Warn] LLM generation failed (non-retryable). Error: {e}")
                        break # Break the retry loop
            
            # *** Proactive Throttle REMOVED ***
            
            # --- End Rate Limit Logic ---

            row = {
                "company_number": company_data['company_number'],
                "company_name": company_name,
                "ground_truth_url": ground_truth_url,
                "scraped_result_position": scraped_pos,
                "scraped_result_url": scraped_url,
                "is_correct_url": is_correct_url,
                "string_match_result": string_match_result,
                "llm_match_result": llm_answer,
                "llm_match_url_in_response": llm_url
            }
            analysis_results.append(row)

    print("\n--- Analysis complete. ---")
    if not analysis_results:
        print("No results to save. Exiting.")
        return

    try:
        df = pd.DataFrame(analysis_results)
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        print(f"✅ Successfully saved {len(df)} analysis rows to **{OUTPUT_CSV}**")
    except Exception as e:
        print(f"❌ Critical Error: Failed to write CSV file. Error: {e}")

if __name__ == "__main__":
    main()