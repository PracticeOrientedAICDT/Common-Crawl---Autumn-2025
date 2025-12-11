import os
import sys
import json
import re
import pandas as pd
from google import genai 
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
import html2text
from typing import List, Dict, Any, Tuple, Optional
import time  
import jellyfish
# --- Configuration ---
INPUT_JSON = "scraper_results.json"
OUTPUT_CSV = "analysis_results_JSON_LLM_TP_Recurse.csv"
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


def parse_llm_output(llm_response: str) -> Dict[str, Any]:
    """
    Parses LLM response and extracts structured fields.
    
    Returns a dict with:
    - is_official_website: bool
    - official_url: str or None
    - found_embedded_link: bool  
    - embedded_url: str or None
    - reasoning: str
    - parse_success: bool (did parsing work?)
    """
    result = {
        "is_official_website": False,
        "official_url": None,
        "found_embedded_link": False,
        "embedded_url": None,
        "reasoning": "",
        "parse_success": False
    }
    
    try:
        # Clean up response - remove markdown code blocks if present
        cleaned = llm_response.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)
        
        # Parse JSON
        parsed = json.loads(cleaned)
        
        # Extract fields
        result["is_official_website"] = bool(parsed.get("is_official_website", False))
        result["official_url"] = parsed.get("official_url")
        result["found_embedded_link"] = bool(parsed.get("found_embedded_link", False))
        result["embedded_url"] = parsed.get("embedded_url")
        result["reasoning"] = parsed.get("reasoning", "")
        result["parse_success"] = True
        
    except (json.JSONDecodeError, KeyError, AttributeError) as e:
        # Parsing failed - store raw response in reasoning
        result["reasoning"] = f"PARSE_ERROR: {llm_response[:200]}"
        result["parse_success"] = False
    
    return result

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
    # You need to tweak the the prompt with the commmented out line 2 below if you are using the CH Dataset in entity 1, otherwise comment out the line below
    sic_code_str = ", ".join(company_data['sic_codes']) if company_data['sic_codes'] else "N/A"
    #SIC codes: {company_data['sic_code_desc']}
    return f"""
You must respond with ONLY valid JSON. No other text before or after.

Your task: Determine if Entity 2 is Entity 1's official website.

Respond with this exact JSON structure:
{{
    "is_official_website": true or false,
    "official_url": "url string or null",
    "found_embedded_link": true or false,
    "embedded_url": "url string or null",
    "reasoning": "brief explanation"
}}
DO NOT set found_embedded_link to true unless you have found a different official website link within the page content of Entity 2 if is officaial_website is false. Never if is_official_website is true.
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

Entity 1:
Company name: {company_data['company_name']}
Company number: {company_data['company_number']}
Address post code: {company_data['postcode']}
SIC codes: {sic_code_str}

Entity 2:
{scraped_content[:15000]} 

Answer in JSON format only:
"""

def validate_embedded_link(company_data: Dict[str, Any], embedded_url: str, markdown_content: str, llm_client) -> Dict[str, Any]:
    """Validates embedded link using rejection logic."""
    
    prompt = f"""
You must respond with ONLY valid JSON.

This URL was found on a company directory as Entity 1's website.

Your task: Find CLEAR reasons this CANNOT be Entity 1's website.

JSON structure:
{{
    "should_reject": true or false,
    "rejection_reasons": ["list"] or [],
    "reasoning": "brief explanation"
}}

ONLY reject if:
1. Explicitly based OUTSIDE UK
2. Business has NO CONNECTION to: {company_data['sic_codes']}
3. Clearly a DIFFERENT company (not variant/parent/subsidiary)

Entity 1:
Company: {company_data['company_name']} ({company_data['company_number']})
Postcode: {company_data['postcode']}
SIC: {company_data['sic_codes']}

Website content:
{markdown_content[:15000]}

JSON Response:
"""
    
    try:
        response = llm_client.models.generate_content(model='gemini-2.5-flash-lite', contents=prompt)
        parsed = parse_rejection_llm_output(response.text)
        
        return {
            "is_official_website": not parsed["should_reject"],
            "official_url": embedded_url if not parsed["should_reject"] else None,
            "found_embedded_link": False,
            "embedded_url": None,
            "reasoning": parsed["reasoning"],
            "parse_success": parsed["parse_success"]
        }
    except Exception as e:
        return {
            "is_official_website": False,
            "official_url": None,
            "found_embedded_link": False,
            "embedded_url": None,
            "reasoning": f"ERROR: {e}",
            "parse_success": False
        }

def parse_rejection_llm_output(llm_response: str) -> Dict[str, Any]:
    """
    Parses LLM response for rejection-based validation.
    """
    result = {
        "should_reject": True,  # Default to reject on parse failure (safer)
        "rejection_reasons": [],
        "reasoning": "",
        "parse_success": False
    }
    
    try:
        # Clean up response - remove markdown code blocks if present
        cleaned = llm_response.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)
        
        # Parse JSON
        parsed = json.loads(cleaned)
        
        # Extract fields
        result["should_reject"] = bool(parsed.get("should_reject", True))
        result["rejection_reasons"] = parsed.get("rejection_reasons", [])
        result["reasoning"] = parsed.get("reasoning", "")
        result["parse_success"] = True
        
    except (json.JSONDecodeError, KeyError, AttributeError) as e:
        # Parsing failed - default to rejection (safer for embedded links)
        result["reasoning"] = f"PARSE_ERROR: {llm_response[:200]}"
        result["parse_success"] = False
    
    return result


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
    recursion_depth={}
    for trial in all_trials_data:
        company_data = trial['ground_truth_data']
        company_name = company_data['company_name']
        ground_truth_url = company_data['ground_truth_url']
        
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

            
            cleaned_scraped_url = clean_base_url(scraped_url)
            is_correct_url = (cleaned_scraped_url == ground_truth_url)

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
            llm_parsed = parse_llm_output(llm_answer)
            if llm_parsed['found_embedded_link']:
                company_number = company_data['company_number']
                if recursion_depth.get(company_number,0) <1:
                    embedded_url = llm_parsed['embedded_url']
                    print(f" [Recursion] Checking :{embedded_url}")

                    try:
                        #
                        embedded_content = ScrapeToMarkdown(embedded_url)
                        if not embedded_content:
                            print(f"    [Warn] Failed to scrape embedded URL: {embedded_url}")
                            recursion_depth[company_number] = 1
                        else:
                            try:    
                               recursive_result = validate_embedded_link(company_data, embedded_url, embedded_content, llm_client)
                               analysis_results.append({
                            "company_number": company_data['company_number'],
                            "company_name": company_name,
                            "scraped_result_position": f"{scraped_pos}_recursive",
                            "scraped_result_url": embedded_url,
                            "string_match_result": URL_similarity_match(company_name, get_domain_fragment(embedded_url)),
                            
                            
                            "llm_is_official_website": recursive_result["is_official_website"],
                            "llm_recursive_official_url": recursive_result["official_url"],
                            "llm_recursive_reasoning": recursive_result.get('reasoning'),
                            "llm_embedded_url": recursive_result["embedded_url"],
                            "recurse_should_reject":recursive_result["should_reject"],
                            "llm_parse_success": recursive_result["parse_success"]
                        })
                            except Exception as e:
                              print(f"    [Warn] Recursive LLM validation failed. Error: {e}")
                        recursion_depth[company_number] = 1
                    except Exception as e:
                        print(f"    [Warn] Recursive LLM validation failed. Error: {e}")
                        recursion_depth [company_number] = 1    

            row = {
                "company_number": company_data['company_number'],
                "company_name": company_name,
                "ground_truth_url": ground_truth_url,
                "scraped_result_position": scraped_pos,
                "scraped_result_url": scraped_url,
                "is_correct_url": is_correct_url,
                "string_match_result": string_match_result,
                "Key_ID_match": Key_ID_match,
                "llm_answer": llm_answer,
                "llm_is_official_website": llm_parsed['is_official_website'],
                "llm_official_url": llm_parsed['official_url'],
                "llm_found_embedded_link": llm_parsed['found_embedded_link'],
                "llm_embedded_url": llm_parsed['embedded_url'],
                "llm_reasoning": llm_parsed['reasoning'],
                "llm_parse_success": llm_parsed['parse_success']
                

                 }
            analysis_results.append(row)
            
    print("\n--- Analysis complete. ---")
    if not analysis_results:
        print("No results to save. Exiting.")
        return

    try:
        df = pd.DataFrame(analysis_results)
        total_companies = df['company_number'].nunique()
        print(f"Preparing to save analysis for {total_companies} unique companies...")
        companies_with_match = df[df['llm_is_official_website'] == True]['company_number'].nunique()
        print(f"Companies with ≥1 match: {companies_with_match}")
        recall_pct = (companies_with_match / total_companies) * 100 if total_companies > 0 else 0
        print(f"Recall: {recall_pct:.1f}%")
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        print(f"✅ Successfully saved {len(df)} analysis rows to **{OUTPUT_CSV}**")
        print(f" total skipped is ={total_skiped}")
    except Exception as e:
        print(f"❌ Critical Error: Failed to write CSV file. Error: {e}")

if __name__ == "__main__":
    main()