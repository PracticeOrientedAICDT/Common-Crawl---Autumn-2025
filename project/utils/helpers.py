import re, json, random, requests, pandas as pd, html2text
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import jellyfish

def ScrapeToMarkdown(url: str):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        for tag in soup(['script','style','nav','footer','header','form','aside']):
            tag.decompose()
        body_content = str(soup.body) if soup.body else str(soup)
        h = html2text.HTML2Text()
        h.ignore_links, h.ignore_images, h.body_width = False, True, 0
        markdown_text = h.handle(body_content)
        return '\n'.join([line.strip() for line in markdown_text.splitlines() if line.strip()])
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
        return None

def SerphSearch(search_string, api_key):
    url = "https://google.serper.dev/search"
    payload = json.dumps({"q": search_string, "location":"United Kingdom","gl":"gb"})
    headers = {'X-API-KEY': api_key, 'Content-Type':'application/json'}
    try:
        r = requests.post(url, headers=headers, data=payload)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f" Search failed: {e}")
        return {"error": str(e)}

def _clean_string(t):
    t = t.lower()
    for s in ['limited liability partnership','limited','ltd','llp']:
        t = t.replace(s,'')
    return re.sub(r'[^a-z0-9]', '', t)

def URL_similarity_match(name, url_frag):
    a, b = _clean_string(name), _clean_string(url_frag)
    if not a: return False
    lev = jellyfish.levenshtein_distance(a, b)
    ratio = (len(a)+len(b)-lev)/(len(a)+len(b)) if (len(a)+len(b))>0 else 1.0
    return ratio >= 0.9

def extract_test_case(row=None):
    FILE = "/Users/mm25873/Documents/Practice Project 1/Companies House data/Trustpilot process/ground_truth_dataset.csv"
    COLS = [1,2,3,12,29,30]
    df = pd.read_csv(FILE, header=0)
    if df.empty: return []
    idx = random.randint(0,len(df)-1) if row is None else row-2
    data = df.iloc[idx, COLS]
    return [str(x) for x in data]

def clean_base_url(raw):
    try:
        p = urlparse(raw if '://' in raw else 'http://'+raw)
        return f"{p.scheme}://{p.netloc}"
    except: return None

def get_domain_fragment(url):
    try:
        n = urlparse(url).netloc
        if n.startswith("www."): n = n[4:]
        return n.split('.')[0]
    except: return ""

def check_html_match(md, name, postcode, company_number=None):
    """
    Check if the HTML/markdown content contains the company info.

    Parameters:
        md (str): The scraped content (markdown or plain text).
        name (str): Company name.
        postcode (str): Company postcode.
        company_number (str, optional): Company number.

    Returns:
        bool: True if any of the key identifiers are found in the content.
    """
    md_lower = md.lower()
    if name.lower() in md_lower:
        return True
    if postcode.lower() in md_lower:
        return True
    if company_number and str(company_number).lower() in md_lower:
        return True
    return False


def create_llm_prompt(company_data: dict, scraped_content: str) -> str:
    """
    Builds a prompt for the LLM to verify if a scraped webpage
    belongs to the given company.

    Args:
        company_data: dict with keys 'company_name', 'company_number', 'postcode', 'sic_codes'
        scraped_content: string (Markdown text scraped from a webpage)

    Returns:
        A formatted LLM prompt as a string.
    """
    sic_codes = ", ".join(company_data.get("sic_codes", [])) or "N/A"
    scraped_text_trunc = scraped_content[:15000]  # truncate long pages

    prompt = f"""
    You are verifying whether a webpage belongs to a specific UK company.

    Company Information:
    - Name: {company_data.get('company_name')}
    - Company Number: {company_data.get('company_number')}
    - Postcode: {company_data.get('postcode')}
    - SIC Codes: {sic_codes}

    Scraped Webpage Content (truncated):
    {scraped_text_trunc}

    Question: Based on the above, does this webpage belong to the company?
    Respond with only "Yes" or "No". Do not include any URLs or explanations.
    """

    return prompt.strip()

def parse_llm_response(response_text: str) -> str:
    """
    Normalizes the LLM response into a clear Yes/No output.
    """
    if not response_text:
        return "No"
    cleaned = response_text.strip().lower()
    if cleaned.startswith("yes"):
        return "Yes"
    return "No"
