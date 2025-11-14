import os
import sys
import json
import re
import pandas as pd
from google import genai 
from urllib.parse import urlparse
from Scrape_Utils import URL_similarity_match
from typing import List, Dict, Any, Tuple
import time

# --- Configuration ---
INPUT_JSON = "scraper_results.json"
OUTPUT_CSV = "analysis_results_embedding.csv"
EMBEDDING_MODEL = "gemini-embedding-001"
SIMILARITY_THRESHOLD = 0.75
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

def create_embedding_input(company_data: Dict[str, Any], markdown_content: str) -> str:
    """Builds the standardized text input for embedding."""
    sic_code_str = ", ".join(company_data['sic_codes']) if company_data['sic_codes'] else "N/A"
    
    return f"""Company: {company_data['company_name']}, Postcode: {company_data['postcode']}, Sectors: {sic_code_str}. Website: {markdown_content[:15000]}"""

def get_embedding(client, text: str) -> List[float] | None:
    """
    Calls the embedding API and returns the vector.
    Includes retry logic for rate limits.
    """
    MAX_RETRIES = 3
    retries = 0
    
    while retries < MAX_RETRIES:
        try:
            response = client.models.embed_content(
                model=EMBEDDING_MODEL,
                contents=text
            )
            # response.embeddings is a list of ContentEmbedding objects
            # Each has a .values attribute containing the vector
            if response.embeddings and len(response.embeddings) > 0:
                return response.embeddings[0].values
            else:
                print(f"    [Warn] Empty embeddings in response.")
                return None
        except Exception as e:
            if "429 RESOURCE_EXHAUSTED" in str(e):
                retries += 1
                wait_time = 60
                print(f"    [Warn] Hit Rate Limit on embedding. Retrying {retries}/{MAX_RETRIES} in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"    [Warn] Embedding generation failed. Error: {e}")
                return None
    
    print(f"    [Warn] Max retries exceeded for embedding.")
    return None

def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """Calculates cosine similarity between two vectors."""
    if not vec1 or not vec2:
        return 0.0
    
    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    magnitude1 = sum(a ** 2 for a in vec1) ** 0.5
    magnitude2 = sum(b ** 2 for b in vec2) ** 0.5
    
    if magnitude1 == 0 or magnitude2 == 0:
        return 0.0
    
    return dot_product / (magnitude1 * magnitude2)

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
        
        # Create company embedding (same for all results in this trial)
        company_embedding_text = f"Company: {company_name}, Postcode: {company_data['postcode']}, Sectors: {', '.join(company_data['sic_codes']) if company_data['sic_codes'] else 'N/A'}"
        company_embedding = get_embedding(llm_client, company_embedding_text)
        
        if company_embedding is None:
            print(f"  [Error] Failed to embed company info. Skipping trial.")
            continue
        
        # Track best match for this trial
        best_similarity = -1
        best_result_idx = -1
        best_result = None
        
        # Embed each scraped result
        for result in trial['scraped_results']:
            scraped_url = result['link']
            scraped_pos = result['position']
            markdown_content = result['markdown_content']
            
            print(f"  Embedding result {scraped_pos}: {scraped_url}")
            
            # Create embedding input
            embedding_input = create_embedding_input(company_data, markdown_content)
            result_embedding = get_embedding(llm_client, embedding_input)
            
            if result_embedding is None:
                print(f"    [Warn] Failed to embed result {scraped_pos}. Assigning 0.0 similarity.")
                similarity = 0.0
            else:
                similarity = cosine_similarity(company_embedding, result_embedding)
            
            print(f"    - Similarity: {similarity:.4f}")
            
            # Track best match
            if similarity > best_similarity:
                best_similarity = similarity
                best_result_idx = scraped_pos
                best_result = result
        
        # Now evaluate if best match passes threshold
        if best_result is None:
            print("  [Error] No valid results to evaluate.")
            continue
        
        cleaned_best_url = clean_base_url(best_result['link'])
        is_correct_url = (cleaned_best_url == ground_truth_url)
        threshold_pass = best_similarity >= SIMILARITY_THRESHOLD
        
        print(f"  Best match: Position {best_result_idx}, Similarity: {best_similarity:.4f}, Passes threshold: {threshold_pass}")
        
        row = {
            "company_number": company_data['company_number'],
            "company_name": company_name,
            "ground_truth_url": ground_truth_url,
            "embedding_best_match_position": best_result_idx,
            "embedding_best_match_url": best_result['link'],
            "embedding_similarity_score": round(best_similarity, 4),
            "embedding_threshold_pass": threshold_pass,
            "is_correct_embedding_match": is_correct_url
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
        
        # Print summary statistics
        correct_matches = df['is_correct_embedding_match'].sum()
        accuracy = (correct_matches / len(df)) * 100 if len(df) > 0 else 0
        threshold_accuracy = (df[df['embedding_threshold_pass'] == True]['is_correct_embedding_match'].sum() / 
                             df[df['embedding_threshold_pass'] == True].shape[0]) if df[df['embedding_threshold_pass'] == True].shape[0] > 0 else 0
        
        print(f"\n--- Summary Statistics ---")
        print(f"Total trials: {len(df)}")
        print(f"Correct matches (best result == ground truth): {correct_matches} ({accuracy:.2f}%)")
        print(f"Results passing threshold: {df['embedding_threshold_pass'].sum()}")
        print(f"Accuracy among threshold-passing results: {threshold_accuracy*100:.2f}%")
        
    except Exception as e:
        print(f"❌ Critical Error: Failed to write CSV file. Error: {e}")

if __name__ == "__main__":
    main()