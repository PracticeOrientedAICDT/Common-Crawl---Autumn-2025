import os
import sys
import json
import re
import pandas as pd
from google import genai 
from urllib.parse import urlparse

from typing import List, Dict, Any, Tuple
import time  
import jellyfish
# --- Configuration ---
INPUT_JSON = "scraper_results_Random_CH.json"
OUTPUT_CSV = "analysis_results_CH.csv"
# ---------------------


# this is my helper function to clean the strings for matching_you don't need it if you aren't matching back to the ground truth.
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
# matches similar strings based on Levenshtein distance.
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
# OK this is another one that will need changes if you are feeeding different versions of the scraped sites.
def check_md_match(markdown_content: str, company_name: str, postcode: str) -> bool:
    has_pos_match = company_name.lower() in markdown_content.lower() or postcode.lower() in markdown_content.lower()
    if not has_pos_match:
        return False
    # add any more company info aggregator sites to this list to exclude if we find them. 
    excl_URL_frags = ['open.endole.co.uk','uk.globaldatabase.com','companywall.co.uk','bringo.co.uk','companiesintheuk.co.uk', 'companycheck.co.uk', 'bizdb.co.uk', 'check-business.co.uk']
    for frag in excl_URL_frags:
        if frag in markdown_content.lower():
            return False
    return True

# ALTER THIS FOR different prompts.  
def create_llm_prompt(company_data: Dict[str, Any], scraped_content: str) -> str:
    """Builds the standardized prompt for the LLM."""
    #sic_code_str = ", ".join(company_data['sic_codes']) if company_data['sic_codes'] else "N/A"
    
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
6. The website should be the official website of the business and be related to the trade described in the SIC codes, be careful it is not a website that is discussing the company only.
7. if Entity 2 contains website URL contains open.endole.co.uk,  please check for the official website link within the page and return that instead if found.
Does the information from entity 2 match the company information 
from entity 1?
Answer with 'Yes' and the corresponding URL if they match, and 
answer with 'No' if they do not.
Or answer LINK and then with the link you have found to the entity 1's wesite, on open.endole.co.uk or similar if there is one. Do not give me a a open.endole.co.uk link at all.
Entity 1:
Company name: {company_data['company_name']}
Company number: {company_data['company_number']}
Address post code: {company_data['postcode']}
SIC codes: {company_data['sic_code_desc']}

Entity 2:
{scraped_content[:15000]} 

Answer:
"""



def main():
    total_skiped = 0
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
        #ground_truth_url = company_data['ground_truth_url']
        
        print(f"\n--- Processing Trial {trial['trial_number']}: {company_name} ---")

        if not trial['scraped_results']:
            print("  [Warn] No scraped results for this trial. Skipping.")
            total_skiped += 1
            continue
            
        for result in trial['scraped_results']:
            scraped_url = result['link']
            scraped_pos = result['position']
            markdown_content = result['markdown_content']
            
            print(f"  Analysing result {scraped_pos}: {scraped_url}")

            #cleaned_scraped_url = clean_base_url(scraped_url)
            #is_correct_url = (cleaned_scraped_url == ground_truth_url)

            domain_fragment = get_domain_fragment(scraped_url)
            string_match_result = URL_similarity_match(company_name, domain_fragment)
            #Match on key identfiers in the marskedown content, exact company name and post code
            Key_ID_match = check_md_match(markdown_content, company_name, company_data['company_number'])
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
                    llm_answer = response.text
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
                #"ground_truth_url": ground_truth_url,
                "scraped_result_position": scraped_pos,
                "scraped_result_url": scraped_url,
                #"is_correct_url": is_correct_url,
                "string_match_result": string_match_result,
                "Key_ID_match": Key_ID_match,
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
        print(f" total skipped is ={total_skiped}")
    except Exception as e:
        print(f"❌ Critical Error: Failed to write CSV file. Error: {e}")

if __name__ == "__main__":
    main()