import requests
import json
from typing import Dict, Any

def SerphSearch(search_string: str, api_key: str) -> Dict[str, Any]:
    """
    Performs a Google search using the Serper API, returning the structured JSON result. first page of results only. 

    Args:
        search_string: The query string to search for (from the LLM or wherever we get it from) .
        api_key: I don't want to put this on GitHub!

    Returns:
        A dictionary containing the structured JSON search results from the API.
        If an error occurs, returns a dictionary with an "error" key.
    """
    
    url = "https://google.serper.dev/search"
    
    # 1. Prepare the payload with the dynamic search string
    payload = json.dumps({
      "q": search_string,
      "location": "United Kingdom",
      "gl": "gb"
    })
    
    # 2. Prepare the headers, using the input api_key
    headers = {
      'X-API-KEY': api_key,
      'Content-Type': 'application/json'
    }

    # 3. Execute the API request
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status() 
        
        # 4. Return the parsed JSON response as a Python dictionary
        return response.json()
        
    except requests.exceptions.HTTPError as http_err:
        # Handle HTTP errors (e.g., 401 Unauthorized for bad API key, 429 Rate Limit)
        print(f"HTTP error occurred: {http_err} - Response: {response.text}")
        return {"error": f"HTTP Error: {http_err}", "details": response.text}
    except requests.exceptions.RequestException as req_err:
        # Handle other request errors (e.g., connection errors, timeouts)
        print(f"Request error occurred: {req_err}")
        return {"error": f"Request Error: {req_err}"}
    
