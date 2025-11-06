from .base_agent import BaseAgent
from utils.helpers import SerphSearch
from utils.config import SERPER_API_KEY, EXCLUDED_DOMAINS
from urllib.parse import urlparse

class SearchAgent(BaseAgent):
    def __init__(self):
        super().__init__("SearchAgent")

    def run(self, input_data):
        company = input_data["company_name"]
        postcode = input_data["postcode"]
        company_no = input_data["company_number"]
        query = f"{company} {postcode} {company_no} company website"
        res = SerphSearch(query, SERPER_API_KEY)
        organic = res.get("organic", [])
        filtered = []
        for r in organic:
            url = r.get("link")
            if not url: continue
            d = urlparse(url).netloc.lower()
            if any(ex in d for ex in EXCLUDED_DOMAINS): continue
            filtered.append(r)
            if len(filtered) == 3: break
        return {"urls": filtered, "search_query": query}
