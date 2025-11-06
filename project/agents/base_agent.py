from abc import ABC, abstractmethod
import logging

class BaseAgent(ABC):
    def __init__(self, name):
        self.name = name
        self.logger = logging.getLogger(name)

    @abstractmethod
    def run(self, input_data: dict) -> dict:
        pass

    def safe_run(self, input_data: dict) -> dict:
        try:
            result = self.run(input_data)
            return {"status": "success", **result}
        except Exception as e:
            self.logger.error(f"{self.name} failed: {e}")
            return {"status": "error", "error": str(e)}
