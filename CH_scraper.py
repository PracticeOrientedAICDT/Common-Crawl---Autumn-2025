import requests
import csv
import time
import json
from datetime import datetime

# Configuration
API_KEY = "b8ca8790-2ffe-453e-a4e5-8e45b935e19a"  # Replace with your actual API key
BASE_URL = "https://api.company-information.service.gov.uk/advanced-search/companies"
OUTPUT_FILE = f"active_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
BATCH_SIZE = 5000
RATE_LIMIT_DELAY = 2  # seconds between requests

# TEST MODE: Set to True to only fetch first few batches for testing
TEST_MODE = True
TEST_BATCHES = 3  # Number of batches to fetch in test mode

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

def main():
    print("Companies House Active Companies Scraper")
    print("=" * 50)
    
    if TEST_MODE:
        print("*** TEST MODE ENABLED ***")
        print(f"Will only fetch {TEST_BATCHES} batches ({TEST_BATCHES * BATCH_SIZE:,} records)")
        print("Set TEST_MODE = False to run full scrape")
        print()
    
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Rate limit delay: {RATE_LIMIT_DELAY}s")
    print()
    
    start_index = 0
    total_retrieved = 0
    batch_number = 1
    write_header = True
    fieldnames_tracker = None
    
    while True:
        # Check if we've hit test mode limit
        if TEST_MODE and batch_number > TEST_BATCHES:
            print()
            print("=" * 50)
            print(f"TEST MODE COMPLETE: {total_retrieved:,} companies saved")
            print("Check the CSV file to verify the data looks correct.")
            print("Set TEST_MODE = False to run the full scrape.")
            break
        
        print(f"Fetching batch {batch_number} (starting at index {start_index})...")
        
        data = fetch_companies(start_index, BATCH_SIZE)
        
        if data is None:
            print("Failed to fetch data. Stopping.")
            break
        
        companies = data.get('items', [])
        total_available = data.get('total_results', 0)
        
        if not companies:
            print("No more companies to retrieve.")
            break
        
        # Write to CSV and track fieldnames
        fieldnames_tracker = write_to_csv(companies, OUTPUT_FILE, write_header=write_header, existing_fieldnames=fieldnames_tracker)
        write_header = False  # Only write header once
        
        total_retrieved += len(companies)
        print(f"✓ Retrieved {len(companies)} companies")
        
        
        
        # Check if we've retrieved everything
        if total_retrieved >= total_available:
            print("All companies retrieved!")
            break
        
        # Move to next batch
        start_index += BATCH_SIZE
        batch_number += 1
        
        # Rate limiting
        print(f"Waiting {RATE_LIMIT_DELAY}s before next request...")
        time.sleep(RATE_LIMIT_DELAY)
    
    print()
    print("=" * 50)
    print(f"COMPLETE: {total_retrieved:,} companies saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()