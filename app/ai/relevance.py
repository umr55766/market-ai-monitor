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
        
    def _get_batch_prompt(self, headlines: list[str]) -> str:
        numbered_list = "\n".join([f"{i+1}. {h}" for i, h in enumerate(headlines)])
        return f"""
        Analyze the following news headlines and determine if each has potential impact on GLOBAL FINANCIAL MARKETS (stocks, commodities, currencies, bonds).
        
        Headlines:
        {numbered_list}
        
        Respond with a numbered list of ONLY "YES" or "NO" for each headline (e.g., "1. YES\n2. NO").
        """

    def is_relevant_batch(self, headlines: list[str]) -> list[bool]:
        if not headlines:
            return []
        try:
            self.rate_limiter.wait()
            
            response = self.client.models.generate_content(
                model=os.getenv("GEMINI_MODEL", "gemma-3-12b-it"),
                contents=self._get_batch_prompt(headlines)
            )
            
            lines = response.text.strip().split("\n")
            results = []
            for line in lines:
                upper_line = line.upper()
                if "YES" in upper_line:
                    results.append(True)
                elif "NO" in upper_line:
                    results.append(False)
            
            # Pad with False if the AI didn't return enough lines
            while len(results) < len(headlines):
                results.append(False)
            
            return results[:len(headlines)]
            
        except Exception as e:
            print(f"Batch AI Filter Error: {e}")
            return [False] * len(headlines)

    def is_relevant(self, headline: str) -> bool:
        return self.is_relevant_batch([headline])[0]
