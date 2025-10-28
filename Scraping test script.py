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
from Scrape_Utils import URL_similarity_match
from Scrape_Utils import extract_test_case
import os
from google import genai

s_api_key = os.environ.get('SERPER_API_KEY')

CURRENTCO = extract_test_case()  # randomly generates a test case from the CSV file
OUTPUT_FILENAME = "search_results.csv"

SEARCH_QUERY = CURRENTCO[2] + CURRENTCO [3] +CURRENTCO [0] + "company website"  # Example: using the first test case

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
g_api_key=os.environ["GOOGLE_API_KEY"]

# 1. Initialize the client.
# It automatically finds the "GEMINI_API_KEY" from your os.environ
try:
    client = genai.Client()
except Exception as e:
    print(f"Error initializing client. Is GEMINI_API_KEY set? \nError: {e}")
    exit()

# 2. Define your entities

entity_2 = all_data[0]['markdown_content']  # Using the first scraped page content

# 3. Create your prompt
prompt = f"""
Your task is to find a companys official website by deciding if the two 
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
Does the information from entity 2 match the company information 
from entity 1?
Answer with ‘Yes’ and the corresponding URL if they match, and 
answer with ‘No’ if they do not.
Entity 1:
Company name: {CURRENTCO[2]}
Company number: {CURRENTCO[0]}
Address post code: {CURRENTCO[3]}
SIC codes:{CURRENTCO[4]+CURRENTCO[5]}
Entity 2: {entity_2}

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