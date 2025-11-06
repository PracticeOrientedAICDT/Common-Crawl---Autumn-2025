import pandas as pd
import numpy as np
import sys

# --- 1. Load and Clean Data ---
print("Loading data... Ensure 'analysis_results.csv' is in the same folder.")
file_path = 'analysis_results_embedding.csv'
try:
    df = pd.read_csv(file_path)
except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
    print("Please make sure this script and the 'analysis_results.csv' file are in the same directory.")
    sys.exit()
except Exception as e:
    print(f"An error occurred loading the file: {e}")
    sys.exit()

def clean_url(url):
    '''Cleans a URL to get the top-level domain.'''
    if not isinstance(url, str):
        return None
    url = url.replace("https://", "").replace("http://", "")
    url = url.replace("www.", "")
    url = url.split('/', 1)[0]
    return url.strip()

print("Cleaning URLs and preparing data...")
df['cleaned_ground_truth_url'] = df['ground_truth_url'].apply(clean_url)
df['cleaned_scraped_result_url'] = df['scraped_result_url'].apply(clean_url)
df['new_url_match'] = (df['cleaned_ground_truth_url'] == df['cleaned_scraped_result_url'])

# Handle potential 'Yes'/'No' strings in llm_match_result
df['llm_match_result_bool'] = df['llm_match_result'].astype(str).str.strip().str.lower() == 'yes'

# --- 2. Define Row-Level Success (NEW LOGIC) ---
print("Defining success metrics per row with new flexible logic...")

# 'is_string_match' is STRICT: requires URL match AND string match
df['is_string_match'] = df['new_url_match'] & df['string_match_result']

# 'is_llm_match' is FLEXIBLE: only requires the LLM to say Yes
df['is_llm_match'] = df['llm_match_result_bool']

# 'is_overall_match' is True if EITHER of the above are True
df['is_overall_match'] = df['is_string_match'] | df['is_llm_match']


# --- 3. Company-Level Aggregation (Success Stats) ---
print("Aggregating success stats by company...")
company_stats = df.groupby('company_number').agg(
    # Was *any* row a string match for this company?
    found_by_string = ('is_string_match', 'any'),
    
    # Was *any* row an LLM match for this company?
    found_by_llm = ('is_llm_match', 'any'),
    
    # Was *any* row an overall success (by either method)?
    found_overall = ('is_overall_match', 'any')
)

# --- 4. Aggregate URLs for Inspection ---
print("Aggregating URLs for manual inspection...")
ground_truth_urls = df.groupby('company_number')['ground_truth_url'].first().rename('ground_truth_url_display')

scraped_urls_pivot = df.pivot(index='company_number', columns='scraped_result_position', values='scraped_result_url')
if 1 not in scraped_urls_pivot.columns: scraped_urls_pivot[1] = np.nan
if 2 not in scraped_urls_pivot.columns: scraped_urls_pivot[2] = np.nan
if 3 not in scraped_urls_pivot.columns: scraped_urls_pivot[3] = np.nan

scraped_urls_pivot = scraped_urls_pivot.rename(columns={
    1: 'scraped_result_url_1',
    2: 'scraped_result_url_2',
    3: 'scraped_result_url_3'
})
scraped_urls_pivot = scraped_urls_pivot[['scraped_result_url_1', 'scraped_result_url_2', 'scraped_result_url_3']]

# --- 5. Aggregate Other Info ---
pos1_stats = df[df['scraped_result_position'] == 1].groupby('company_number')['is_overall_match'].any().rename('pos1_found_match')
company_names = df.groupby('company_number')['company_name'].first()

# --- 6. Join All Aggregations ---
print("Joining all data...")
final_df = company_stats.join(company_names.to_frame())
final_df = final_df.join(ground_truth_urls.to_frame())
final_df = final_df.join(scraped_urls_pivot)
final_df = final_df.join(pos1_stats.to_frame())
final_df['pos1_found_match'] = final_df['pos1_found_match'].fillna(False)

# --- 7. Re-order Columns and Save ---
print("Re-ordering columns and saving...")
final_columns = [
    'company_name',
    'ground_truth_url_display', 
    'scraped_result_url_1',   
    'scraped_result_url_2',   
    'scraped_result_url_3',   
    'found_overall',
    'found_by_string',
    'found_by_llm',
    'pos1_found_match'
]

other_cols = [col for col in final_df.columns if col not in final_columns]
final_df = final_df[final_columns + other_cols]

# Save to NEW CSV
output_file = 'company_level_analysis_embed.csv'
final_df.reset_index(inplace=True) # Move company_number from index to a column
final_df.to_csv(output_file, index=False)

print(f"\n--- Successfully created file: {output_file} ---")
print("This file uses the new FLEXIBLE LLM logic.")
print("Filter by 'found_overall' (True = Match, False = Miss) to manually inspect the misses.")
