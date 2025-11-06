# from .base_agent import BaseAgent
# import pandas as pd
# import json
# from urllib.parse import urlparse
# from utils.config import OUTPUT_CSV, OUTPUT_JSON

# class EvaluatorAgent(BaseAgent):
#     def __init__(self):
#         super().__init__("EvaluatorAgent")

#     @staticmethod
#     def _base_url(url: str) -> str:
#         """Normalize URL for comparison: scheme + domain, remove www, ignore trailing slashes"""
#         if not url:
#             return ""
#         try:
#             parsed = urlparse(url if '://' in url else 'http://' + url)
#             netloc = parsed.netloc.lower()
#             if netloc.startswith("www."):
#                 netloc = netloc[4:]
#             return f"{parsed.scheme}://{netloc}"
#         except:
#             return url.lower()

#     def _is_correct_selection(self, ground_truth: str, selected: str) -> bool:
#         return self._base_url(ground_truth) == self._base_url(selected)

#     def run(self, input_data):
#         trials = input_data["trials"]
#         processed = []

#         for trial in trials:
#             new_trial = {}

#             gt = trial.get("ground_truth_data", {})
#             # urls = trial.get("urls", [])  # top 3 search URLs from SearchAgent
#             first_attempt_urls = trial.get("urls_first_attempt", [])
#             retry_attempt_urls = trial.get("urls_retry_attempt", [])
#             selected_url = trial.get("selected_url", "")

#             # --- Core company info ---
#             new_trial["trial_number"] = trial.get("trial_number")
#             new_trial["company_number"] = gt.get("company_number")
#             new_trial["company_name"] = gt.get("company_name")
#             new_trial["postcode"] = gt.get("postcode")
#             new_trial["sic_codes"] = ", ".join(gt.get("sic_codes", []))
#             new_trial["ground_truth_url"] = gt.get("ground_truth_url")

#             # --- Selected URL & match method ---
#             selected_url = trial.get("selected_url", "")
#             new_trial["selected_url"] = selected_url
#             new_trial["match_method"] = trial.get("match_method", "None")

#             # --- Correct selection based on normalized URL comparison ---
#             new_trial["correct_selection"] = self._is_correct_selection(
#                 gt.get("ground_truth_url", ""), selected_url
#             )

#             # --- LLM verdict / confidence if available ---
#             new_trial["llm_verdict"] = trial.get("llm_match_result", "")
#             new_trial["llm_confidence"] = trial.get("llm_confidence", "")

#             # --- Retry info ---
#             new_trial["retry_used"] = bool(retry_attempt_urls)

#             # --- Top 3 search URLs from first attempt ---
#             for i in range(3):
#                 if i < len(first_attempt_urls):
#                     new_trial[f"search_url_first_attempt_{i+1}"] = first_attempt_urls[i]
#                 else:
#                     new_trial[f"search_url_first_attempt_{i+1}"] = ""

#             # --- Top 3 search URLs from retry attempt ---
#             for i in range(3):
#                 if i < len(retry_attempt_urls):
#                     new_trial[f"search_url_retry_{i+1}"] = retry_attempt_urls[i]
#                 else:
#                     new_trial[f"search_url_retry_{i+1}"] = ""

#             # # --- Top 3 search URLs ---
#             # for i in range(3):
#             #     if i < len(urls):
#             #         new_trial[f"search_url_{i+1}"] = urls[i].get("link", "")
#             #     else:
#             #         new_trial[f"search_url_{i+1}"] = ""

#             processed.append(new_trial)

#         # --- Save CSV ---
#         df = pd.DataFrame(processed)
#         df.to_csv(OUTPUT_CSV, index=False)

#         # --- Save JSON ---
#         with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
#             json.dump(processed, f, indent=2)

#         return {
#             "summary": f"Saved {len(df)} trials to CSV & JSON with top 3 search URLs and correct_selection."
#         }

from .base_agent import BaseAgent
import pandas as pd
import json
from urllib.parse import urlparse
from utils.config import OUTPUT_CSV, OUTPUT_JSON

class EvaluatorAgent(BaseAgent):
    def __init__(self):
        super().__init__("EvaluatorAgent")

    @staticmethod
    def _base_url(url):
        """Normalize URL for comparison: keep scheme + netloc, remove www."""
        if not url:
            return ""
        try:
            parsed = urlparse(url if '://' in url else 'http://' + url)
            netloc = parsed.netloc.lower()
            if netloc.startswith("www."):
                netloc = netloc[4:]
            return f"{parsed.scheme}://{netloc}"
        except:
            return url.lower()

    def is_correct_selection(self, ground_truth: str, selected: str) -> bool:
        return self._base_url(ground_truth) == self._base_url(selected)

    def run(self, input_data):
        trials = input_data["trials"]
        processed = []

        for trial in trials:
            new_trial = {}
            gt = trial.get("ground_truth_data", {})

            # --- Core company info ---
            new_trial["trial_number"] = trial.get("trial_number")
            new_trial["company_name"] = gt.get("company_name")
            new_trial["company_number"] = gt.get("company_number")
            new_trial["postcode"] = gt.get("postcode")
            new_trial["sic_codes"] = ", ".join(gt.get("sic_codes", []))
            new_trial["ground_truth_url"] = gt.get("ground_truth_url")

            # --- Selected URL & match method ---
            selected_url = trial.get("selected_url", "")
            new_trial["selected_url"] = selected_url
            new_trial["match_method"] = trial.get("match_method", "None")
            new_trial["correct_selection"] = self.is_correct_selection(
                gt.get("ground_truth_url", ""), selected_url
            )

            # --- Attempt tracking ---
            attempts = trial.get("attempts", [])
            for idx, att in enumerate(attempts, start=1):
                # Top URLs per attempt
                for u_idx, url in enumerate(att.get("urls", []), start=1):
                    new_trial[f"attempt{idx}_url{u_idx}"] = url
                # If fewer than 3 URLs, fill empty
                for u_idx in range(len(att.get("urls", [])) + 1, 4):
                    new_trial[f"attempt{idx}_url{u_idx}"] = ""

                # Selected URL & match method per attempt
                new_trial[f"attempt{idx}_selected_url"] = att.get("selected_url", "")
                new_trial[f"attempt{idx}_match_method"] = att.get("match_method", "")

            # --- Flag if second attempt was used ---
            new_trial["used_retry"] = len(attempts) > 1

            processed.append(new_trial)

        # --- Save CSV ---
        df = pd.DataFrame(processed)
        df.to_csv(OUTPUT_CSV, index=False)

        # --- Save JSON ---
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(processed, f, indent=2)

        return {
            "summary": f"Saved {len(df)} trials to CSV & JSON. Includes top URLs per attempt, selected URL, match method, and retry flag."
        }
