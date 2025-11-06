import os
from dotenv import load_dotenv

load_dotenv()

SERPER_API_KEY = os.environ.get("SERPER_API_KEY")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")

if not SERPER_API_KEY or not GOOGLE_API_KEY:
    raise ValueError("SERPER_API_KEY or GOOGLE_API_KEY not set in .env file!")

NUM_TRIALS = 100
OUTPUT_JSON = "/Users/mm25873/Documents/Practice Project 1/projectoutput/scraper_results_agentic.json"
OUTPUT_CSV = "/Users/mm25873/Documents/Practice Project 1/projectoutput/scraper_results_agentic.csv"

EXCLUDED_DOMAINS = [
    ".gov.uk",
    ".gov",
    "open.endole.co.uk",
    "find-and-update.company-information.service.gov.uk",
    "northdata.com"
]
