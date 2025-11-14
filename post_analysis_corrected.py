import pandas as pd
import numpy as np
import sys

# --- 1. Load and Clean Data ---
print("Loading data... Ensure 'analysis_results.csv' is in the same folder.")
file_path = 'analysis_results_Key_ID.csv'
try:
    df = pd.read_csv(file_path)
except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
    print("Please make sure this script and the 'analysis_results.csv' file are in the same directory.")
    sys.exit()
except Exception as e:
    print(f"An error occurred loading the file: {e}")
    sys.exit()

def clean_url_to_core_domain(url):
    '''Extracts just the core domain name from a URL.
    
    Strips: protocol, www, subdomains, TLD, paths, query params
    Keeps: hyphens in domain names
    
    Examples:
        https://www.art-of-brewing.co.uk/?param=x  → art-of-brewing
        open.endole.co.uk/contact                   → endole
        uk.trustpilot.com/review/...                → trustpilot
        www.board-game.co.uk                        → board-game
        https://www.three.co.uk/privacy             → three
    '''
    if not isinstance(url, str):
        return None
    
    # 1. Remove protocol
    url = url.replace("https://", "").replace("http://", "")
    
    # 2. Remove path and query string
    url = url.split('/')[0]
    url = url.split('?')[0]
    
    # 3. Remove www. prefix if present
    if url.startswith("www."):
        url = url[4:]
    
    # 4. Split by dots to identify TLD and extract core domain
    parts = url.split('.')
    
    if len(parts) <= 1:
        return url.strip().lower() if url.strip() else None
    
    if len(parts) == 2:
        # Simple case: domain.com
        return parts[0].strip().lower()
    
    # Multiple parts: could be subdomain.domain.tld or domain.co.uk
    # Common two-part TLDs: co.uk, com.au, etc.
    two_part_tlds = {'co', 'com', 'org', 'net', 'gov', 'edu', 'ac'}
    
    if len(parts) >= 3 and parts[-2] in two_part_tlds:
        # Last two parts are TLD (e.g., .co.uk)
        # Core domain is everything before that
        core = '.'.join(parts[:-2])
    else:
        # Last part is TLD, take second-to-last as core domain
        core = parts[-2]
    
    return core.strip().lower()

print("Cleaning URLs to core domain and preparing data...")
df['cleaned_ground_truth_domain'] = df['ground_truth_url'].apply(clean_url_to_core_domain)
df['cleaned_scraped_domain'] = df['scraped_result_url'].apply(clean_url_to_core_domain)
df['domain_match'] = (df['cleaned_ground_truth_domain'] == df['cleaned_scraped_domain'])

# Handle potential 'Yes'/'No' strings in llm_match_result
df['llm_match_result_bool'] = df['llm_match_result'].astype(str).str.strip().str.lower() == 'yes'

# --- 2. Define Row-Level Success ---
print("Defining success metrics per row...")

# Main match criteria: domain must match
df['is_match'] = df['domain_match']

# Keep other methods as informational columns for debugging
df['string_match_bool'] = df['string_match_result'].astype(bool)
df['Key_ID_match_bool'] = df['Key_ID_match'].astype(bool)
df['llm_match_bool'] = df['llm_match_result_bool']
df['is_match'] = (
    df['domain_match'] &  # <-- & means 'AND'
    (
        df['string_match_bool'] |  # <-- | means 'OR'
        df['llm_match_bool'] |
        df['Key_ID_match_bool']
    )
)
# --- 3. Company-Level Aggregation ---
print("Aggregating success stats by company...")
company_stats = df.groupby('company_number').agg(
    # Was *any* row a match (domain match)?
    found_by_domain = ('is_match', 'any'),
    
    # For reference: was it found by string matching?
    found_by_string = ('string_match_bool', 'any'),
    #For reference: did Key ID matching work?
    found_by_Key_ID = ('Key_ID_match_bool', 'any'),
    # For reference: did LLM say yes on any row?
    found_by_llm = ('llm_match_bool', 'any'),
)

# --- 4. Aggregate URLs for Inspection ---
print("Aggregating URLs for manual inspection...")
ground_truth_urls = df.groupby('company_number')['ground_truth_url'].first().rename('ground_truth_url')
ground_truth_domains = df.groupby('company_number')['cleaned_ground_truth_domain'].first().rename('cleaned_ground_truth_domain')

scraped_urls_pivot = df.pivot_table(index='company_number', columns='scraped_result_position', values='scraped_result_url', aggfunc='first')
if 1 not in scraped_urls_pivot.columns: scraped_urls_pivot[1] = np.nan
if 2 not in scraped_urls_pivot.columns: scraped_urls_pivot[2] = np.nan
if 3 not in scraped_urls_pivot.columns: scraped_urls_pivot[3] = np.nan

scraped_urls_pivot = scraped_urls_pivot.rename(columns={
    1: 'scraped_result_url_pos1',
    2: 'scraped_result_url_pos2',
    3: 'scraped_result_url_pos3'
})
scraped_urls_pivot = scraped_urls_pivot[['scraped_result_url_pos1', 'scraped_result_url_pos2', 'scraped_result_url_pos3']]

# Also pivot cleaned domains for easier inspection
scraped_domains_pivot = df.pivot_table(index='company_number', columns='scraped_result_position', values='cleaned_scraped_domain', aggfunc='first')
if 1 not in scraped_domains_pivot.columns: scraped_domains_pivot[1] = np.nan
if 2 not in scraped_domains_pivot.columns: scraped_domains_pivot[2] = np.nan
if 3 not in scraped_domains_pivot.columns: scraped_domains_pivot[3] = np.nan

scraped_domains_pivot = scraped_domains_pivot.rename(columns={
    1: 'cleaned_scraped_domain_pos1',
    2: 'cleaned_scraped_domain_pos2',
    3: 'cleaned_scraped_domain_pos3'
})
scraped_domains_pivot = scraped_domains_pivot[['cleaned_scraped_domain_pos1', 'cleaned_scraped_domain_pos2', 'cleaned_scraped_domain_pos3']]

# --- 5. Track position of first match ---
print("Tracking first match position...")
first_match_pos = df[df['is_match']].groupby('company_number')['scraped_result_position'].min().rename('first_match_position')

# --- 6. Aggregate other info ---
company_names = df.groupby('company_number')['company_name'].first()

# --- 7. Join All Aggregations ---
print("Joining all data...")
final_df = company_stats.join(company_names.to_frame())
final_df = final_df.join(ground_truth_urls)
final_df = final_df.join(ground_truth_domains)
final_df = final_df.join(scraped_urls_pivot)
final_df = final_df.join(scraped_domains_pivot)
final_df = final_df.join(first_match_pos)

# --- 8. Re-order and format columns ---
print("Re-ordering columns and saving...")
final_columns = [
    'company_name',
    'ground_truth_url',
    'cleaned_ground_truth_domain',
    'scraped_result_url_pos1',
    'cleaned_scraped_domain_pos1',
    'scraped_result_url_pos2',
    'cleaned_scraped_domain_pos2',
    'scraped_result_url_pos3',
    'cleaned_scraped_domain_pos3',
    'found_by_domain',
    'first_match_position',
    'found_by_Key_ID',
    'found_by_string',
    'found_by_llm',
]

final_df = final_df[final_columns]

# Save to CSV
output_file = 'company_level_analysis_Key_ID.csv'
final_df.reset_index(inplace=True)  # Move company_number from index to a column
final_df.to_csv(output_file, index=False, encoding='utf-8')

print(f"\n{'='*70}")
print(f"Successfully created: {output_file}")
print(f"{'='*70}")
print("\nKey columns:")
print("  - found_by_domain: TRUE if any scraped result matched ground truth domain")
print("  - first_match_position: Position (1, 2, or 3) where domain was first found")
print("  - found_by_string: For reference - string similarity matches")
print("  - found_by_llm: For reference - LLM acceptances")
print("\nFilter by 'found_by_domain = False' to investigate false negatives")
print("(which will likely show ground truth URL inconsistencies)")
