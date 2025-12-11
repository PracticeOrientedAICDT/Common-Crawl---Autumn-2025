import os
import sys
import json
from urllib.parse import urlparse
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup
from typing import Optional, List, Dict, Any
import html2text
import re
import requests
import json
import pandas as pd

# scrape to markdown function
def ScrapeToText(url: str) -> Optional[str]:
    """
    Fetches the content of a given URL, cleans the HTML, and converts 
    the body content into a plain text string suitable for an LLM.

    Args:
        url: The URL of the webpage to scrape.

    Returns:
        A string containing the page content in plain text format, or None on failure.
    
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

        # 3. Extract content from specific tags
        tags_extract = ['title', 'address', 'header', 'footer']
        extracted_content = []
        
        for tag_name in tags_extract:
            tags = soup.find_all(tag_name)
            for tag in tags:
                text = tag.get_text(separator=" ", strip=True)
                if text:
                    extracted_content.append(text)
        
        # 4. Combine extracted content into plain text
        if extracted_content:
            return " ".join(extracted_content)
        else:
            print(f"No content found in specified tags for {url}")
            return None

    except requests.exceptions.HTTPError as e:
        print(f"Error fetching {url}: HTTP Error - {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: Connection/Request Error - {e}")
        return None
    except requests.exception.Timeout as e:
        print(f"Error fetching {url}: Timeout Error - {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

# # for testing:
# if __name__ == "__main__":
#     url = "https://www.cginsurance.com" # example URL
#     result = ScrapeToText(url)
#     if result:
#         print(result)
#     else:
#         print("Failed to fetch or process the URL.")