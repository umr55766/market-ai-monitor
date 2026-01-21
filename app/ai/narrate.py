from google import genai
import os
from typing import List, Dict

class AlertNarrator:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")
        
        self.client = genai.Client(api_key=self.api_key)
        self.model = os.getenv("GEMINI_MODEL", "gemma-3-12b-it")
    
    def narrate_alert(self, anomaly: Dict, correlations: List[Dict]) -> str:
        """
        Generate a concise, professional alert narrative for a market anomaly.
        
        Args:
            anomaly: Dict with ticker, change_pct, score, level
            correlations: List of correlated news items
        
        Returns:
            A 2-3 sentence alert message
        """
        ticker = anomaly['ticker']
        change_pct = anomaly['change_pct']
        level = anomaly['level']
        
        # Build context from correlated news
        news_context = ""
        if correlations:
            news_titles = [n['title'] for n in correlations[:2]]  # Top 2 news
            news_context = f"\n\nRelated news:\n" + "\n".join([f"- {t}" for t in news_titles])
        
        prompt = f"""You are a financial market analyst. Generate a concise, professional alert message (2-3 sentences max) for the following market event:

Asset: {ticker}
Price Change: {change_pct:+.2f}%
Severity: {level}
{news_context}

Write a clear, actionable alert that explains what happened and why it matters. Be direct and professional.

Also include ONE explicit recommended next step, phrased as: "Next step: ...".
The next step should be specific and immediately actionable (e.g., check related news, verify if the move is headline-driven vs broader market, review exposure/hedges, set an alert level, or wait for confirmation if appropriate)."""

        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt
            )
            return response.text.strip()
        except Exception as e:
            # Fallback to simple message if AI fails
            return (
                f"🚨 {level} Alert: {ticker} moved {change_pct:+.2f}%. "
                f"Next step: review the top related headlines and check whether this move is sector-wide or idiosyncratic before taking action."
            )
