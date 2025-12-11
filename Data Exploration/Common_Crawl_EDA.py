import pandas as pd
from urllib.parse import urlparse
from collections import Counter
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0  # For consistent language detection

# Path to your CSV
csv_file = "/Users/mm25873/Documents/Practice Project 1/Common Crawl Data/df2024.csv" 

# Load the CSV
df = pd.read_csv(csv_file)

print("\nColumns in this dataset:")
print(df.columns.tolist())

# Drop rows without URL or content
df = df.dropna(subset=['url', 'content'])

# Basic counts
total_pages = len(df)
total_words = df['content'].apply(lambda x: len(str(x).split())).sum()
total_chars = df['content'].apply(lambda x: len(str(x))).sum()
average_words = total_words / total_pages if total_pages else 0
average_chars = total_chars / total_pages if total_pages else 0

# Domain analysis
df['domain'] = df['url'].apply(lambda x: urlparse(str(x)).netloc)
top_domains = df['domain'].value_counts().head(10).to_dict()

# Duplicate analysis
duplicate_pages = df.duplicated(subset=['url']).sum()  # duplicates based on URL

# Unique websites
unique_websites = df['parent_url'].nunique()

# Pages flagged as info pages
info_pages_count = df['is_info_page'].sum()

# Pages with very short content (<50 words)
short_pages_count = df['content'].apply(lambda x: len(str(x).split()) < 50).sum()

# Distribution of pages per website (parent_url)
pages_per_website = df.groupby('parent_url').size()
website_distribution_summary = {
    'max_pages_per_site': pages_per_website.max(),
    'min_pages_per_site': pages_per_website.min(),
    'median_pages_per_site': pages_per_website.median(),
    'mean_pages_per_site': pages_per_website.mean()
}

# Summary output
summary = {
    'total_pages': total_pages,
    'total_words': total_words,
    'total_characters': total_chars,
    'average_words_per_page': average_words,
    'average_chars_per_page': average_chars,
    'top_domains': top_domains,
    'duplicate_pages': duplicate_pages,
    'unique_websites': unique_websites,
    'info_pages_count': info_pages_count,
    'short_pages_count': short_pages_count,
    'website_distribution_summary': website_distribution_summary
}

print("Common Crawl CSV Summary:")
for k, v in summary.items():
    print(f"{k}: {v}")
