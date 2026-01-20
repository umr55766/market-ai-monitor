from google import genai
import os
import time
from app.ai.utils import RateLimiter

class RelevanceFilter:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")
        
        self.client = genai.Client(api_key=self.api_key)
        # Target 30 RPM
        self.rate_limiter = RateLimiter(rpm=30)
        
    def _get_prompt(self, headline: str) -> str:
        return f"""
        Analyze the following news headline and determine if it has potential impact on GLOBAL FINANCIAL MARKETS (stocks, commodities, currencies, bonds).
        
        Headline: "{headline}"
        
        Respond with ONLY one word: "YES" or "NO".
        """

    def is_relevant(self, headline: str) -> bool:
        try:
            self.rate_limiter.wait()
            
            response = self.client.models.generate_content(
                model=os.getenv("GEMINI_MODEL", "gemma-3-12b-it"),
                contents=self._get_prompt(headline)
            )
            answer = response.text.strip().upper().replace('*', '')
            
            return "YES" in answer
        except Exception as e:
            print(f"AI Filter Error for '{headline}': {e}")
            return False
