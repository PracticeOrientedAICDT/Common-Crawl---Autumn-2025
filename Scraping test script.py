import requests
import os
import json
import re
import pandas as pd
from typing import Dict, Any
from SerphAPI import SerphSearch
from Scrape_Utils import ScrapeToMarkdown
from Scrape_Utils import search_and_scrape
from Scrape_Utils import SerphSearch
import os
from google import genai

s_api_key = os.environ.get('SERPER_API_KEY')

SEARCH_QUERY = "HGB CONSULTING LIMITED E15 4QR company website"
OUTPUT_FILENAME = "search_results.csv"


if __name__ == "__main__":
    # Ensure the API key is set
    if not s_api_key:
        print("Error: SERPER_API_KEY environment variable not set.")
    else:
        # Call the main function to get the data
        all_data = search_and_scrape(SEARCH_QUERY, s_api_key)
        
        if all_data:
            print("\n==============================================")
            print(f"✅ Process finished. Scraped {len(all_data)} pages.")
            print("Data structure (showing titles and filenames):")
            
            for item in all_data:
                print(f"  - Pos {item['position']}: {item['title']} ({item['filename']})")
                print(f"    Content length: {len(item['markdown_content'])} chars")
            
            # You would now pass 'all_data' to your LLM function, e.g.:
            # llm_response = ask_gemini_with_context(all_data)
            
        else:
            print("\n==============================================")
            print("❌ Process finished. No data was scraped.")

entity_2 =all_data[0]['markdown_content']  # Example: using the first scraped page content
print(entity_2)
# Configure with your API key (using environment variable)
g_api_key=os.environ["GEMINI_API_KEY"]

# 1. Initialize the client.
# It automatically finds the "GEMINI_API_KEY" from your os.environ
try:
    client = genai.Client()
except Exception as e:
    print(f"Error initializing client. Is GEMINI_API_KEY set? \nError: {e}")
    exit()

# 2. Define your entities
entity_1 = SEARCH_QUERY
entity_2 = all_data[0]['markdown_content']  # Using the first scraped page content

# 3. Create your prompt
prompt = f"""
You are an entity matching expert. Compare the two entities below.
Entity 1 is extracted from companies house data, Entity 2 is extracted from web scraped data and cleaned data from web searches for the company.
Respond with only'Yesthey are the same entity' or 'No' if they are not.

Entity 1: "{entity_1}"
Entity 2: "{entity_2}"
Answer:
"""

# 4. Generate content using the CLIENT
#    This is the main fix:
response = client.models.generate_content(
    model='gemini-2.5-flash-lite',  # Pass the model name as an argument
    contents=prompt             # Pass the prompt to the 'contents' argument
)

print(response.text.strip())

# Example Output:
# Yes