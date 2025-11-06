# from agents.search_agent import SearchAgent
# from agents.scraper_agent import ScraperAgent
# from agents.matcher_agent import MatcherAgent
# from agents.verifier_agent import VerifierAgent
# from agents.evaluator_agent import EvaluatorAgent
# from agents.retry_agent import RetryAgent
# from utils.helpers import extract_test_case, clean_base_url
# from utils.config import NUM_TRIALS
# from google import genai

# class Orchestrator:
#     def __init__(self):
#         self.search = SearchAgent()
#         self.scraper = ScraperAgent()
#         self.matcher = MatcherAgent()
#         self.verifier = VerifierAgent()
#         self.evaluator = EvaluatorAgent()
#         self.cache = {}
#         self.llm_client = genai.Client()
#         self.retry = RetryAgent(self.llm_client)

#     def run(self):
#         trials = []
#         for i in range(NUM_TRIALS):
#             print(f"\n--- Trial {i+1}/{NUM_TRIALS} ---")
#             tc = extract_test_case()
#             if not tc or len(tc) < 6: 
#                 continue

#             company_number, gt_url_raw, company_name, postcode, sic1, sic2 = tc
#             gt_url = clean_base_url(gt_url_raw)
#             sic_codes = [s for s in [sic1, sic2] if str(s).lower() != 'nan']
#             company = {
#                 "company_name": company_name, 
#                 "company_number": company_number, 
#                 "postcode": postcode, 
#                 "sic_codes": sic_codes, 
#                 "ground_truth_url": gt_url}

#             s = self.search.safe_run(company)
#             # top_urls = s.get("urls", [])
#             sc = self.scraper.safe_run({"urls": s["urls"], "cache": self.cache})
#             m = self.matcher.safe_run({"scraped_data": sc["scraped_data"], "target": company})
#             v = self.verifier.safe_run({"scraped_data": sc["scraped_data"], "selected_url": m["selected_url"], "target": company})

#             selected_url = m["selected_url"] or v["llm_url"]
#             match_method = m["match_method"] if m["selected_url"] else "LLM" if v["llm_result"] == "Yes" else "None"

#             trial = {
#                 "trial_number": i+1,
#                 "ground_truth_data": company,    # include full company info
#                 "urls": s["urls"],               # top 3 search results from SearchAgent
#                 "selected_url": selected_url,
#                 "match_method": match_method,
#                 "urls_first_attempt": [u.get("link") for u in s["urls"]]
#             }
#             # --- Second attempt only if first failed ---
#             if not selected_url:
#                 trial = self.retry.safe_run({
#                     "trial": trial,
#                     "company": company,
#                     "used_urls": set(trial["urls_first_attempt"])
#                 })

#             trials.append(trial)
#         self.evaluator.safe_run({"trials": trials})

from agents.search_agent import SearchAgent
from agents.scraper_agent import ScraperAgent
from agents.matcher_agent import MatcherAgent
from agents.verifier_agent import VerifierAgent
from agents.evaluator_agent import EvaluatorAgent
from agents.retry_agent import RetryAgent
from utils.helpers import extract_test_case, clean_base_url
from utils.config import NUM_TRIALS
from google import genai

MAX_ATTEMPTS = 2  # first + one retry

class Orchestrator:
    def __init__(self):
        self.search = SearchAgent()
        self.scraper = ScraperAgent()
        self.matcher = MatcherAgent()
        self.verifier = VerifierAgent()
        self.evaluator = EvaluatorAgent()
        self.retry = RetryAgent(genai.Client())
        self.cache = {}

    def run(self):
        trials = []

        for i in range(NUM_TRIALS):
            print(f"\n--- Trial {i+1}/{NUM_TRIALS} ---")
            tc = extract_test_case()
            if not tc or len(tc) < 6:
                continue

            company_number, gt_url_raw, company_name, postcode, sic1, sic2 = tc
            gt_url = clean_base_url(gt_url_raw)
            sic_codes = [s for s in [sic1, sic2] if str(s).lower() != 'nan']
            company = {
                "company_name": company_name,
                "company_number": company_number,
                "postcode": postcode,
                "sic_codes": sic_codes,
                "ground_truth_url": gt_url
            }

            # Initialize trial state
            trial_state = {
                "trial_number": i + 1,
                "ground_truth_data": company,
                "urls_tried": set(),
                "selected_url": "",
                "match_method": "None",
                "attempts": []
            }

            attempt = 1
            while attempt <= MAX_ATTEMPTS and not trial_state["selected_url"]:
                print(f"--- Attempt {attempt} ---")

                # --- Generate or reuse search URLs ---
                if attempt == 1:
                    search_result = self.search.safe_run(company)
                    new_urls = search_result["urls"]
                    trial_state["initial_query"] = search_result.get("search_query", "")
                else:
                    retry_result = self.retry.safe_run({
                        "trial": trial_state,
                        "company": company,
                        "used_urls": trial_state["urls_tried"]
                    })
                    # new_urls = retry_result["new_urls"]
                    # trial_state["retry_query"] = retry_result.get("new_search_query", "")
                    new_urls = retry_result.get("new_urls", [])
                    trial_state = retry_result.get("trial", trial_state)
                    # trial_state = retry_result["trial"]

                # Filter out URLs already tried
                # new_urls = [u for u in new_urls if u["link"] not in trial_state["urls_tried"]]
                trial_state["urls_tried"].update([u["link"] for u in new_urls])
                trial_state[f"attempt_{attempt}_urls"] = [u["link"] for u in new_urls]

                # --- Scrape and match ---
                scraped = self.scraper.safe_run({"urls": new_urls, "cache": self.cache})
                matcher_res = self.matcher.safe_run({"scraped_data": scraped["scraped_data"], "target": company})
                verifier_res = self.verifier.safe_run({
                    "scraped_data": scraped["scraped_data"],
                    "selected_url": matcher_res.get("selected_url", ""),
                    "target": company
                })

                selected_url = matcher_res.get("selected_url") or verifier_res.get("llm_url", "")
                match_method = matcher_res.get("match_method") if matcher_res.get("selected_url") else \
                               "LLM" if verifier_res.get("llm_result") == "Yes" else "None"

                trial_state["selected_url"] = selected_url
                trial_state["match_method"] = match_method
                trial_state["attempts"].append({
                    "attempt": attempt,
                    "urls": [u["link"] for u in new_urls],
                    "selected_url": selected_url,
                    "match_method": match_method
                })

                attempt += 1

            trials.append(trial_state)

        # --- Save results ---
        self.evaluator.safe_run({"trials": trials})


    def run_one(self, company_name, company_number, postcode, sic_codes):
        """Run the full agentic process for one company manually (for Gradio)."""
        print(f"\n--- Single Company Run ---")

        gt_url = ""  # unknown in manual mode
        company = {
            "company_name": company_name,
            "company_number": company_number,
            "postcode": postcode,
            "sic_codes": sic_codes,
            "ground_truth_url": gt_url
        }

        s = self.search.safe_run(company)
        sc = self.scraper.safe_run({"urls": s["urls"], "cache": self.cache})
        m = self.matcher.safe_run({"scraped_data": sc["scraped_data"], "target": company})
        v = self.verifier.safe_run({
            "scraped_data": sc["scraped_data"],
            "selected_url": m.get("selected_url", ""),
            "target": company
        })

        selected_url = m.get("selected_url") or v.get("llm_url")
        match_method = (
            m.get("match_method") if m.get("selected_url")
            else "LLM" if v.get("llm_result") == "Yes"
            else "None"
        )

        result = {
            "company_name": company_name,
            "company_number": company_number,
            "postcode": postcode,
            "selected_url": selected_url,
            "match_method": match_method,
            "llm_verdict": v.get("llm_result", ""),
        }

        print("✅ Result:", result)
        return result

    

# edit correct selection logic - so it does its own check 