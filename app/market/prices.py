import yfinance as yf
import time
from typing import Dict, List

class MarketData:
    def __init__(self, tickers: List[str] = None):
        # Default core trackers
        self.tickers = tickers or ["^GSPC", "GC=F", "CL=F", "BTC-USD", "EURUSD=X"]
        
    def fetch_latest(self) -> Dict[str, float]:
        """Fetch latest prices for configured tickers."""
        results = {}
        for ticker in self.tickers:
            try:
                data = yf.Ticker(ticker)
                # Get current price (regularMarketPrice or similar)
                # fast_info is efficient for current price
                price = data.fast_info['lastPrice']
                results[ticker] = price
            except Exception as e:
                print(f"Error fetching {ticker}: {e}")
        return results

if __name__ == "__main__":
    # Quick test
    m = MarketData()
    print("Testing Price Fetching...")
    print(m.fetch_latest())
