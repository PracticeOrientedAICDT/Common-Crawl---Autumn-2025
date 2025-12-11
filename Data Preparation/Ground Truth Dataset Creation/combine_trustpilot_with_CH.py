import pandas as pd

# Load scraped results
scraped = pd.read_csv("/Users/mm25873/Documents/Practice Project 1/Companies House data/scraped_company_numbers.csv")

# Load Companies House data
ch = pd.read_csv("/Users/mm25873/Documents/Practice Project 1/Companies House data/companies_house_cleaned.csv", dtype={" CompanyNumber": str})


# Normalize company number formats (zero-pad to 8 digits for consistency)
scraped["company_number"] = scraped["company_number"].astype(str).str.zfill(8)
ch[" CompanyNumber"] = ch[" CompanyNumber"].astype(str).str.zfill(8)

# Merge using the different column names
merged = scraped.merge(ch, left_on="company_number", right_on=" CompanyNumber", how="left")

# Check matches
matched = merged[merged[" CompanyNumber"].notnull()]
print(f"✅ {len(matched)} of {len(scraped)} scraped companies matched Companies House records")

# Save enriched dataset
merged.to_csv("/Users/mm25873/Documents/Practice Project 1/Companies House data/scraped_enriched_companies.csv", index=False)
