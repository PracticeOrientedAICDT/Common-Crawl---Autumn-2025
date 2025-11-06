from .base_agent import BaseAgent
from utils.helpers import URL_similarity_match, check_html_match, get_domain_fragment

class MatcherAgent(BaseAgent):
    def __init__(self):
        super().__init__("MatcherAgent")

    def run(self, input_data):
        scraped = input_data["scraped_data"]
        company = input_data["target"]
        selected_url, method = "", "None"

        for r in scraped:
            u = r.get("link")
            if not u: continue
            if URL_similarity_match(company["company_name"], get_domain_fragment(u)):
                selected_url, method = u, "String"
                break
            if r.get("markdown_content") and check_html_match(r["markdown_content"], company["company_name"], company["postcode"]):
                selected_url, method = u, "HTML"
                break
        return {"selected_url": selected_url, "match_method": method}
