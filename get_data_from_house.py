# imports 

import pandas as pd
import json
import time
import os, requests

import requests
import csv
import time
import json
from datetime import datetime




# configs 
people_txt_path = "/home/ae25872/codebase/proai/Common-Crawl---Autumn-2025/psc-snapshot-2025-10-14_1of30 (1)/psc-snapshot-2025-10-14_1of30.txt"

psc_path = "/home/ae25872/codebase/proai/Common-Crawl---Autumn-2025/psc-snapshot-2025-10-14_1of30 (1)/psc-snapshot-2025-10-14_1of30.txt"

out_path = "/home/ae25872/codebase/proai/Common-Crawl---Autumn-2025/psc_flat_1000.csv"
BASE = "https://api.company-information.service.gov.uk"  # or https://api-sandbox.company-information.service.gov.uk
API_KEY = "b8ca8790-2ffe-453e-a4e5-8e45b935e19a" 
#please dont mis-use this key, here only for ease of use. 
BASE_URL = "https://api.company-information.service.gov.uk/advanced-search/companies"
OUTPUT_FILE = f"active_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
BATCH_SIZE = 5000
RATE_LIMIT_DELAY = 2  # seconds between requests

# TEST MODE: Set to True to only fetch first few batches for testing
TEST_MODE = True
TEST_BATCHES = 3 
session = requests.Session()


#functions

def fetch_companies(start_index, size):
    """Fetch a batch of companies from the API"""
    params = {
        'company_status': 'active',
        'size': size,
        'start_index': start_index
    }
    
    try:
        response = requests.get(
            BASE_URL,
            params=params,
            auth=(API_KEY, ''),  # API key as username, empty password
            timeout=30
        )
        
        # Detailed error reporting
        if response.status_code == 401:
            print("\n⚠️  AUTHENTICATION ERROR:")
            print("   - Check your API key is correct")
            print("   - Make sure it's a REST API key (not Streaming API)")
            print("   - Verify the key is registered at: https://developer.company-information.service.gov.uk/")
            print(f"   - Response: {response.text}")
            return None
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print(f"Response body: {e.response.text}")
        return None

def flatten_dict(d, parent_key='', sep='_'):
    """Flatten nested dictionaries for CSV compatibility"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to JSON strings
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)

def write_to_csv(companies, filename, write_header=False, existing_fieldnames=None):
    """Append companies to CSV file"""
    if not companies:
        return existing_fieldnames
    
    # Flatten all companies
    flattened_companies = [flatten_dict(company) for company in companies]
    
    # Collect all fieldnames from this batch
    batch_fieldnames = set()
    for company in flattened_companies:
        batch_fieldnames.update(company.keys())
    
    # Merge with existing fieldnames if this isn't the first batch
    if existing_fieldnames is not None:
        all_fieldnames = existing_fieldnames.union(batch_fieldnames)
    else:
        all_fieldnames = batch_fieldnames
    
    # Sort for consistency
    fieldnames = sorted(all_fieldnames)
    
    mode = 'w' if write_header else 'a'
    
    with open(filename, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore', restval='')
        
        if write_header:
            writer.writeheader()
        
        writer.writerows(flattened_companies)
    
    return all_fieldnames

def fetch_full_company_profile(company_number: str, max_retries: int = 3) -> dict | None:
    """Fetch complete company profile from API with rate-limit handling"""
    url = f"{BASE}/company/{company_number}"
    
    for attempt in range(max_retries):
        try:
            r = session.get(url, timeout=10)
            
            if r.status_code == 200:
                return r.json()
            
            elif r.status_code == 429:
                retry_after = r.headers.get('Retry-After')
                if retry_after:
                    wait = int(retry_after)
                else:
                    wait = 2 ** (attempt + 1)
                
                print(f"  ⚠️  Rate limited. Waiting {wait}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait)
            
            elif r.status_code == 404:
                return None
            
            else:
                print(f"  ✗ HTTP {r.status_code}: {r.text[:100]}")
                return None
                
        except Exception as e:
            print(f"  ✗ Request error: {e}")
            if attempt < max_retries - 1:
                time.sleep(1 + attempt)
            
    return None

def enrich_flat_with_api_data(df, delay=0.6, max_retries=3):
    """
    Enrich existing rows in df by fetching API data for each company_number.
    Adds new columns from API response without adding new rows.
    """
    api_columns_initialized = False
    api_column_list = []
    
    print(f"Enriching {len(df)} rows with API data...")
    print(f"Base delay: {delay}s, Max retries: {max_retries}")
    print("-" * 60)
    
    for i, idx in enumerate(df.index, 1):
        company_num = str(df.loc[idx, 'company_number']).strip()
        
        print(f"[{i}/{len(df)}] Fetching {company_num}...", end=" ")
        
        profile = fetch_full_company_profile(company_num, max_retries=max_retries)
        
        if profile:
            # Flatten the API response
            flat_profile = pd.json_normalize(profile, sep='.')
            
            # First successful fetch: initialize columns
            if not api_columns_initialized:
                api_column_list = list(flat_profile.columns)
                for col in api_column_list:
                    if col not in df.columns:
                        df[col] = None
                api_columns_initialized = True
                print(f"✓ (initialized {len(api_column_list)} API columns)")
            else:
                print("✓")
            
            # Update this row with API data - FIXED: handle lists/dicts properly
            for col in api_column_list:
                if col in flat_profile.columns:
                    val = flat_profile[col].iloc[0]
                    
                    # Convert lists and dicts to JSON strings for safe storage
                    if isinstance(val, (list, dict)):
                        df.at[idx, col] = json.dumps(val)
                    else:
                        df.at[idx, col] = val
        else:
            print("✗ No data")
        
        # Rate limiting
        if i < len(df):
            time.sleep(delay)
    
    print("-" * 60)
    print(f"✓ Complete! Final shape: {df.shape}")
    return df


def main():
    session.auth = (API_KEY, "")


    # Usage
    with open(people_txt_path, 'r') as f:
        data = [json.loads(line) for line in f]
    people_df = pd.DataFrame(data)

    try:
        # NDJSON fast path
        chunk = next(pd.read_json(psc_path, lines=True, chunksize=1000))
        flat = pd.json_normalize(chunk.to_dict(orient='records'), sep='.')
    except ValueError:
        # Fallback: TSV/CSV with a JSON 'data' column
        tsv = pd.read_csv(psc_path, sep="\t", nrows=10000)
        if 'data' in tsv.columns:
            parsed = []
            for x in tsv['data']:
                if isinstance(x, str):
                    try:
                        parsed.append(json.loads(x))
                    except Exception:
                        parsed.append({})
                elif isinstance(x, dict):
                    parsed.append(x)
                else:
                    parsed.append({})
            flat = pd.json_normalize(parsed, sep='.')
            if 'company_number' in tsv.columns and 'company_number' not in flat.columns:
                flat['company_number'] = tsv['company_number'].values[:len(flat)]
        else:
            flat = tsv  # best-effort
        flat.to_csv(out_path, index=False)



    flat = enrich_flat_with_api_data(flat, delay=0.6, max_retries=3)



    # Save enriched data
    flat.to_csv("/home/ae25872/codebase/proai/Common-Crawl---Autumn-2025/psc_flat_500_enriched.csv", index=False)


    return 




if __name__ == "__main__":
    main()