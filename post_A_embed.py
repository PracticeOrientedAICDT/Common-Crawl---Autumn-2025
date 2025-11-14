import pandas as pd
import sys

# --- 1. Load and Clean Data ---
print("Loading data... Ensure 'analysis_results_embedding.csv' is in the same folder.")
file_path = 'analysis_results_embedding.csv'
try:
    df = pd.read_csv(file_path)
except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
    print("Please make sure this script and the 'analysis_results_embedding.csv' file are in the same directory.")
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
df['cleaned_embedding_best_match_url'] = df['embedding_best_match_url'].apply(clean_url)
df['embedding_url_match'] = (df['cleaned_ground_truth_url'] == df['cleaned_embedding_best_match_url'])

# --- 2. Define Row-Level Success ---
print("Defining success metrics per row...")

# 'is_embedding_match': True if URL matches AND threshold passes
df['is_embedding_match'] = df['embedding_url_match'] & df['embedding_threshold_pass']

# --- 3. Rename/Select Columns for Compatibility ---
print("Preparing output columns...")
final_df = df[['company_number', 'company_name', 'ground_truth_url', 'embedding_best_match_url', 
               'embedding_best_match_position', 'embedding_similarity_score', 'embedding_threshold_pass',
               'is_embedding_match']].copy()

final_df = final_df.rename(columns={
    'ground_truth_url': 'ground_truth_url_display',
    'embedding_best_match_url': 'embedding_best_match_url_display',
    'is_embedding_match': 'found_by_embedding'
})

# --- 4. Save ---
print("Saving output...")
output_file = 'company_level_analysis_embedding.csv'
final_df.to_csv(output_file, index=False)

print(f"\n--- Successfully created file: {output_file} ---")

# --- 5. Print Summary Statistics ---
total_companies = len(final_df)
found_by_embedding = final_df['found_by_embedding'].sum()
embedding_accuracy = (found_by_embedding / total_companies * 100) if total_companies > 0 else 0

print(f"\n--- Summary Statistics ---")
print(f"Total companies: {total_companies}")
print(f"Found by embedding: {found_by_embedding}")
print(f"Embedding accuracy: {embedding_accuracy:.2f}%")
print(f"Threshold pass rate: {final_df['embedding_threshold_pass'].sum()}/{total_companies}")
print(f"\nFilter by 'found_by_embedding' (True = Match, False = Miss) to inspect results.")