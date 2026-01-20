from google import genai
from typing import List, Optional
import os
import json
from app.ai.utils import RateLimiter

class EventExtractor:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")
        
        self.client = genai.Client(api_key=self.api_key)
        self.model = os.getenv("GEMINI_MODEL", "gemma-3-12b-it")
        self.rate_limiter = RateLimiter(rpm=30)

    def _get_prompt(self, headline: str) -> str:
        return f"""
        Extract structured geopolitical and market data from the following news headline.
        
        Headline: "{headline}"
        
        Respond ONLY with a valid JSON object matching this schema:
        {{
            "event_type": "The category of the event (e.g., Geopolitical, Macroeconomic, Corporate, Regulatory)",
            "affected_assets": ["List of assets likely impacted like S&P500, Gold, EUR/USD, Oil, etc."],
            "impact_direction": "The potential direction of market impact (Bullish, Bearish, or Volatile)",
            "certainty_score": 0.0 to 1.0 representing your confidence in this extraction
        }}
        """

    def extract_event(self, headline: str) -> Optional[dict]:
        try:
            self.rate_limiter.wait()
            
            response = self.client.models.generate_content(
                model=self.model,
                contents=self._get_prompt(headline)
            )
            
            # Clean response text in case of markdown blocks
            text = response.text.strip()
            if text.startswith("```json"):
                text = text[7:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            print(f"Raw extraction response for '{headline}': {text}", flush=True)
            return json.loads(text)
        except Exception as e:
            print(f"Extraction Error for '{headline}': {e}", flush=True)
            return None
