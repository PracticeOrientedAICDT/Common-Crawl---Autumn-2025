#Ground Truth Dataset Creation

This details the process to create the ground truth dataset. The files invovled were:

- Trustpilot_companies.csv = This file is too large to upload
- Trustpilot_match.py = This was used to scrape the comany numbers from the trustpilot dataset
- scraped_company_number.csv = Contains the output of above script (trustpilot data with company numbers)
- combine_trustpilot_with_CH.py = This was used to match these websites with the company house record using the company number
- scraped_enriched_comapnies.py= This is the output of the above script
- groudn_truth_dataset.csv = After some cleaning and processing of the above csv we have the ground truth dataset
