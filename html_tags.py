from bs4 import BeautifulSoup
import pandas as pd
import requests
from requests.exceptions import SSLError, ConnectionError

# include file type in input/output filenames (include .csv)
# will process all entries in input file if not specified in num_entries
def extract_html_tags(input_filename, num_entries = None, output_filename = "output.csv"):
    
    # load csv file
    df = pd.read_csv(input_filename)
    
    # number of entries from input file to process
    if num_entries == None:
        df_subset = df
        print("Using all rows from the input file")
    else:
        df_subset = df.iloc[:num_entries] # using only first 'num_entries' rows
        print(f"Using first {num_entries} rows from the input file")
        
    # add columns for selected HTML tags
    selected_tags = ["address", "title", "header", "footer"]
    for tag in selected_tags:
        df_subset[tag + "_tag"] = ""
        
    # iterate through URLs in dataframe
    for url in df_subset["parent_url"]:
        
        # add https:// if url does not start with 'https://' to get valid url
        if url.startswith("https://") == True:
            fetch_url = url
        else:
            fetch_url = "https://" + url
            
        # get HTML content
        try:
            response = requests.get(fetch_url)
            soup = BeautifulSoup(response.text, "html.parser")
            
        # skip urls that give SSL or connection errors (websites don't exist)
        except SSLError:
            print(f"SSL Error for URL: {fetch_url}")
            continue
        except ConnectionError:
            print(f"Connection Error for URL: {fetch_url}")
            continue
        
        # get desired HTML tags' content and add to dataframe
        for tag_name in selected_tags:
            found_tags = soup.find_all(tag_name)
            
            tag_text = []
            for t in found_tags:
                # get text from tag, remove extra whitespace
                text = t.get_text(separator = " ", strip = True)
                text = " ".join(text.split())
                if text:
                    tag_text.append(text)
                    
            # add tag text to dataframe, join multiple tag entries with " | "        
            df_subset.loc[df_subset["parent_url"] == url, tag_name + "_tag"] = " | ".join(tag_text)

    # save dataframe to csv file
    df_subset.to_csv(output_filename, index = False)
    print(f"Saved output to {output_filename}")