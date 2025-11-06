from .base_agent import BaseAgent
from utils.helpers import create_llm_prompt, parse_llm_response, ScrapeToMarkdown
from google import genai

class VerifierAgent(BaseAgent):
    def __init__(self, use_local=False):
        super().__init__("VerifierAgent")
        self.use_local = use_local
        if not use_local:
            self.client = genai.Client()

    def run(self, input_data):
        if input_data.get("selected_url"):
            return {"llm_url": input_data["selected_url"], "llm_result": "N/A"}

        company = input_data["target"]
        scraped = input_data["scraped_data"]

        for r in scraped:
            url = r.get("link")
            md = r.get("markdown_content")
            if not md: continue
            prompt = create_llm_prompt(company, md)
            if self.use_local:
                verdict = "No"  # placeholder
            else:
                try:
                    res = self.client.models.generate_content(model="gemini-2.5-flash-lite", contents=prompt)
                    verdict = parse_llm_response(res.text)
                except Exception as e:
                    verdict = "Error"
            if verdict == "Yes":
                return {"llm_url": url, "llm_result": verdict}
        return {"llm_url": "", "llm_result": "No"}
