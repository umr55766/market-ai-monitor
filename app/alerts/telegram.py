import requests
import os
from typing import Optional

class TelegramBot:
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set")
        
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}"
    
    def send_message(self, text: str, parse_mode: Optional[str] = None) -> bool:
        """
        Send a message to the configured Telegram chat.
        
        Args:
            text: Message text to send
            parse_mode: Optional formatting mode ('Markdown' or 'HTML')
        
        Returns:
            True if successful, False otherwise
        """
        url = f"{self.api_url}/sendMessage"
        
        payload = {
            "chat_id": self.chat_id,
            "text": text
        }
        
        if parse_mode:
            payload["parse_mode"] = parse_mode
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            print(f"✓ Telegram message sent successfully", flush=True)
            return True
        except Exception as e:
            print(f"✗ Telegram send failed: {e}", flush=True)
            return False
    
    def send_alert(self, ticker: str, change_pct: float, level: str, narrative: str) -> bool:
        """
        Send a formatted market alert.
        
        Args:
            ticker: Asset ticker symbol
            change_pct: Percentage change
            level: Severity level (HIGH, CRITICAL)
            narrative: AI-generated narrative
        
        Returns:
            True if successful, False otherwise
        """
        emoji = "🔴" if level == "CRITICAL" else "🟠"
        
        message = f"""{emoji} *{level} MARKET ALERT*

*Asset:* {ticker}
*Change:* {change_pct:+.2f}%

{narrative}"""
        
        return self.send_message(message, parse_mode="Markdown")
