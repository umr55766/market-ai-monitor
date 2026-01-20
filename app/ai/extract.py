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

    def _get_batch_prompt(self, headlines: List[str]) -> str:
        numbered_list = "\n".join([f"{i+1}. {h}" for i, h in enumerate(headlines)])
        return f"""
        Extract structured geopolitical and market data from the following news headlines.
        
        Headlines:
        {numbered_list}
        
        Respond with ONLY a valid JSON array containing {len(headlines)} objects (one for each headline in order), matching this schema for each element:
        {{
            "event_type": "Geopolitical, Macroeconomic, Corporate, or Regulatory",
            "affected_assets": ["S&P500", "Gold", etc.],
            "impact_direction": "Bullish, Bearish, or Volatile",
            "certainty_score": 0.0 to 1.0
        }}
        """

    def extract_events_batch(self, headlines: List[str]) -> List[Optional[dict]]:
        if not headlines:
            return []
        try:
            self.rate_limiter.wait()
            
            response = self.client.models.generate_content(
                model=self.model,
                contents=self._get_batch_prompt(headlines)
            )
            
            text = response.text.strip()
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            
            results = json.loads(text)
            
            if not isinstance(results, list):
                results = [results]
            
            while len(results) < len(headlines):
                results.append(None)
            
            return results[:len(headlines)]
            
        except Exception as e:
            print(f"Batch Extraction Error: {e}", flush=True)
            return [None] * len(headlines)

    def extract_event(self, headline: str) -> Optional[dict]:
        return self.extract_events_batch([headline])[0]
