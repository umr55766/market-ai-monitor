import time
from typing import List, Optional
from app.storage.sqlite_db import DashboardDB

class AnomalyDetector:
    def __init__(self, db: DashboardDB, threshold: float = 0.01):
        self.db = db
        self.threshold = threshold  # 1% move by default

    def detect_anomalies(self) -> List[dict]:
        latest_prices = self.db.get_latest_prices()
        anomalies = []
        
        for ticker, current_price in latest_prices.items():
            history = self.db.get_price_history(ticker, limit=10) # Look back at last 10 snapshots
            if len(history) < 2:
                continue
            
            # Simple comparison with the previous snapshot
            prev_price = history[1]['price']
            change = (current_price - prev_price) / prev_price
            
            if abs(change) >= self.threshold:
                anomalies.append({
                    "ticker": ticker,
                    "current_price": current_price,
                    "prev_price": prev_price,
                    "change_pct": change * 100,
                    "timestamp": history[0]['timestamp']
                })
        
        return anomalies

    def correlate_with_news(self, anomaly: dict) -> List[dict]:
        # Look for news events within the last 4 hours that mention the asset
        all_news = self.db.get_recent(limit=100)
        correlations = []
        
        asset_keywords = {
            "^GSPC": ["S&P 500", "US Stocks", "Stock Market", "Wall Street", "Equity"],
            "GC=F": ["Gold", "XAU", "Precious Metals"],
            "BTC-USD": ["Bitcoin", "BTC", "Crypto", "Cryptocurrency"],
            "CL=F": ["Crude Oil", "Brent", "Energy", "OPEC"],
            "EURUSD=X": ["Euro", "EUR", "Forex", "Currency"]
        }
        
        keywords = asset_keywords.get(anomaly['ticker'], [])
        anomaly_time = anomaly['timestamp']
        
        for news in all_news:
            if news['status'] != 'relevant' or not news['event']:
                continue
            
            # Check for keyword match in title or event data
            match = False
            title = news['title'].lower()
            event_assets = str(news['event'].get('affected_assets', [])).lower()
            
            for kw in keywords:
                if kw.lower() in title or kw.lower() in event_assets:
                    match = True
                    break
            
            if match:
                # Check time proximity (within 4 hours)
                time_diff = abs(anomaly_time - news['timestamp']) / 3600
                if time_diff <= 4:
                    correlations.append(news)
                    
        return correlations
