import requests
import os
import json
import re
import pandas as pd
from typing import Dict, Any
from SerphAPI import SerphSearch
from Scrape_Utils import ScrapeToMarkdown

s_api_key = os.environ.get('SERPER_API_KEY')

SEARCH_QUERY = "CARLISLE REFRIGERATION LIMITED WA14 1DW company website"
OUTPUT_FILENAME = "search_results.csv"

def convert_json_to_csv(json_data: Dict[str, Any], filename: str):
    """
    Converts the nested search JSON data into a flat CSV file using pandas.
    """
    # Check for the primary list of results, often 'organic' or 'news'
    if 'organic' not in json_data:
        print("Error: JSON data does not contain the 'organic' results list needed for CSV conversion.")
        return

    # Use json_normalize to flatten the list of dictionaries
    # This automatically handles nested fields by creating new columns (e.g., 'title', 'snippet', 'sitelinks.0.title')
    try:
        df = pd.json_normalize(json_data['organic'])
        
        # Clean up column names by replacing dots with underscores
        df.columns = df.columns.str.replace('.', '_', regex=False)

        # Save the DataFrame to a CSV file
        df.to_csv(filename, index=False, encoding='utf-8')
        
        print(f"Successfully saved {len(df)} records to **{filename}**")
        print("Top 5 columns created:", list(df.columns[:5]))
        
    except Exception as e:
        print(f"An error occurred during CSV conversion: {e}")


if __name__ == "__main__":
    print(f"Starting search for: '{SEARCH_QUERY}'...")
    # print(s_api_key)
    # 1. Call the SerphSearch function to get the JSON result
    results_json = SerphSearch(SEARCH_QUERY, s_api_key)
    
    # 2. Check if the search was successful
    if 'error' not in results_json:
        # 3. Convert the returned JSON (Python dictionary) to a CSV file
        convert_json_to_csv(results_json, OUTPUT_FILENAME)
    else:
        print("Script terminated due to a search error.")
    
    if results_json.get('organic'):
            # 3. Get the first result and extract its URL
            first_result = results_json['organic'][0]
            url_to_scrape = first_result.get('link')
            
            if url_to_scrape:
                print(f"\n--- Now scraping first result: '{first_result.get('title')}' ---")
                print(f"URL: {url_to_scrape}")
                
                # 4. Call the scraping function from Scrape_Utils
                markdown_content = ScrapeToMarkdown(url_to_scrape)
                
                # 5. Print the scraped content (or a failure message)
                if markdown_content:
                    print("\n Scraping Successful. Content (first 500 chars):")
                    print("-------------------------------------------------")
                    print(markdown_content[:500] + "...")
                    print("-------------------------------------------------")
                    cleaned_url = re.sub(r'^https?://', '', url_to_scrape)
                    safe_filename = re.sub(r'[^a-zA-Z0-9]', '_', cleaned_url)
                    markdown_filename = f"{safe_filename}.md"
                    try:
                        with open(markdown_filename, 'w', encoding='utf-8') as f:
                         f.write(markdown_content)
                         print(f"✅ Content successfully saved to file: '{markdown_filename}'")
                    except IOError as e:
                        print(f"❌ Error saving file: {e}")
                       
                else:
                    print("Failed to scrape the webpage.")
            else:
                print("Could not find a URL link in the first search result.")
    else:
            print("No organic search results found to scrape.")
