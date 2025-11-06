from .base_agent import BaseAgent
from utils.helpers import ScrapeToMarkdown

class ScraperAgent(BaseAgent):
    def __init__(self):
        super().__init__("ScraperAgent")

    def run(self, input_data):
        urls = input_data.get("urls", [])
        cache = input_data.get("cache", {})
        results = []
        for u in urls:
            link = u.get("link")
            if not link: continue
            if link in cache:
                md = cache[link]
            else:
                md = ScrapeToMarkdown(link)
                if md: cache[link] = md
            u["markdown_content"] = md or ""
            results.append(u)
        return {"scraped_data": results, "cache": cache}
