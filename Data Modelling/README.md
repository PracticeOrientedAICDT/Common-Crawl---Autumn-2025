# Data Modelling

Purpose: This folder documents the data modelling work for the overall pipeline for linking Companies House entities to their official websites. The focus is on creating the core modelling components: entity matching strategies, web-search blocking, HTML-content scraping, and LLM prompt design.

Files in this folder:
- Search_scrape_P1 -  Script takes in company info and then formulates searches, performs searches, saves output to a JSON file. 
- Matching_P1 -  Takes as input the JSON, then performs the matching process, outputs a .csv with results.
- Matching_with_recursion -  is still a little experimental, but adds the capability to then scrape any found embedded links, this needs tuning, but the funciontalty works.
- Scrape_Utils - conatins a lot of functions that were developed for this project, not all of them are still used, but they all provide potentially useful capabilities. 
- html_tags - function to scrape the selected content of a website and convert to plain text for use in Search_scrape_P1
- matching_prompts - Word document containing the different prompt designs for the LLM matching strategy used in Matching_P1
- scraper_results_Random_CH.json - Is an example of the output from Search_scrape_P1 for a random CH sample.  
