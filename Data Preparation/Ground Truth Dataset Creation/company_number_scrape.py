import re
import requests
import pandas as pd
import tldextract
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm

# Regex for UK-style company numbers (6–8 digits, sometimes prefixed)
COMPANY_REGEX = re.compile(r"(?:Company\s*(?:No\.?|Number)?\s*[:\-]?\s*)(\d{6,8})", re.IGNORECASE)

# --- Load website list ---
websites = pd.read_csv("/Users/mm25873/Documents/Practice Project 1/TrustpilotData/trustpilot_companies.csv")["website"].dropna().unique().tolist()

results = []

def get_html(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            return r.text
    except Exception:
        return None
    return None

def find_company_number(text):
    match = COMPANY_REGEX.search(text)
    return match.group(1) if match else None

def get_useful_links(base_url, html):
    soup = BeautifulSoup(html, "html.parser")
    links = [a.get("href") for a in soup.find_all("a", href=True)]
    keywords = ["about", "contact", "legal", "imprint", "terms", "privacy"]
    useful = []
    for link in links:
        if any(k in link.lower() for k in keywords):
            full = urljoin(base_url, link)
            if tldextract.extract(full).registered_domain == tldextract.extract(base_url).registered_domain:
                useful.append(full)
    return list(set(useful))

for site in tqdm(websites[:1000]):  # limit for testing
    homepage = f"http://{site}" if not site.startswith("http") else site
    html = get_html(homepage)
    if not html:
        continue

    # Try homepage
    num = find_company_number(html)
    if num:
        results.append({"website": site, "company_number": num, "source_url": homepage})
        continue

    # Try linked pages
    links = get_useful_links(homepage, html)
    for link in links:
        sub_html = get_html(link)
        if not sub_html:
            continue
        num = find_company_number(sub_html)
        if num:
            results.append({"website": site, "company_number": num, "source_url": link})
            break

pd.DataFrame(results).to_csv("/Users/mm25873/Documents/Practice Project 1/TrustpilotData/scraped_company_numbers.csv", index=False)
print(f"✅ Saved {len(results)} company numbers")
