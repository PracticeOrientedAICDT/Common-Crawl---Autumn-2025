from .base_agent import BaseAgent
from utils.helpers import SerphSearch, ScrapeToMarkdown, clean_base_url
from utils.config import SERPER_API_KEY, EXCLUDED_DOMAINS
from urllib.parse import urlparse


# Doesnt seem to work - not sure why?? 
# class RetryAgent(BaseAgent):
#     """
#     Agentic retry: If the original selected_url was empty / None ,
#     generate a better search string using company data, fetch new URLs,
#     scrape them, and select again.
#     """
#     def __init__(self, llm_client):
#         super().__init__("RetrytAgent")
#         self.llm_client = llm_client

#     @staticmethod
#     def filter_new_urls(urls, used_urls):
#         """Remove URLs already tried"""
#         return [u for u in urls if u.get("link") not in used_urls]

#     def run(self, input_data):
#         trial = input_data["trial"]
#         company = input_data["company"]
#         used_urls = input_data.get("used_urls", set())

#         # --- Step 1: Generate improved search string via LLM ---
#         llm_prompt = f"""
#         You are a search assistant. Generate a better search query for finding the company website.
#         Company info:
#         - Name: {company['company_name']}
#         - Company number: {company['company_number']}
#         - Postcode: {company['postcode']}
#         - SIC codes: {', '.join(company['sic_codes']) if company['sic_codes'] else 'N/A'}

#         Generate a concise search query for finding the official website.
#         """
#         try:
#             response = self.llm_client.models.generate_content(
#                 model="gemini-2.5-flash-lite",
#                 contents=llm_prompt
#             )
#             search_query = getattr(response, "text", None)
#             if not search_query:
#                 raise ValueError("LLM returned no text")
#             search_query = search_query.strip()
#         except Exception as e:
#             print(f"LLM search string generation failed: {e}")
#             search_query = f"{company['company_name']} {company['postcode']} company website"

#         # --- Step 2: Run Serper search ---
#         res = SerphSearch(search_query, SERPER_API_KEY)
#         organic = res.get("organic", [])
#         filtered = []
#         for r in organic:
#             url = r.get("link")
#             if not url:
#                 continue
#             d = urlparse(url).netloc.lower()
#             if any(ex in d for ex in EXCLUDED_DOMAINS):
#                 continue
#             filtered.append(r)
#             if len(filtered) == 3:
#                 break

#         # --- Step 3: Remove already tried URLs ---
#         new_urls = self.filter_new_urls(filtered, used_urls)
#         if not new_urls:
#             return trial  # nothing new to try

#         # --- Step 4: Scrape new URLs ---
#         for u in new_urls:
#             md = ScrapeToMarkdown(u["link"])
#             u["markdown_content"] = md or ""

#         # --- Step 5: Re-run matching logic (simple: string match + HTML check) ---
#         selected_url = ""
#         match_method = "None"
#         for result in new_urls:
#             url = result.get("link")
#             if not url:
#                 continue
#             domain_frag = urlparse(url).netloc.split('.')[0]
#             if company['company_name'].lower().replace(" ", "") in domain_frag.lower().replace(" ", ""):
#                 selected_url = url
#                 match_method = "String"
#                 break
#             if url and result.get("markdown_content") and company['company_name'].lower() in result["markdown_content"].lower():
#                 selected_url = url
#                 match_method = "HTML"
#                 break

#         # --- Step 6: Update trial record ---
#         trial["selected_url"] = selected_url or trial.get("selected_url")
#         trial["match_method"] = match_method if selected_url else trial.get("match_method")
#         trial["search_urls_retry"] = [u.get("link") for u in new_urls]

#         if not new_urls:
#             return {
#                 "trial": trial,
#                 "new_urls": [u.get("link") for u in new_urls]
#             }

class RetryAgent(BaseAgent):
    def __init__(self, llm_client):
        super().__init__("RetryAgent")
        self.llm_client = llm_client

    def run(self, input_data):
        """Placeholder RetryAgent — returns empty results safely."""
        trial = input_data.get("trial", {})
        print("⚠️ RetryAgent placeholder called — skipping retry process.")

        # Just return the expected structure so orchestrator doesn’t break
        return {
            "trial": trial,
            "new_urls": [],
            "new_search_query": ""
        }
