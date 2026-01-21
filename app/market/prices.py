from typing import Dict, List, Optional

class MarketData:
    def __init__(self, tickers: List[str] = None):
        # Default core trackers
        self.tickers = tickers or ["^GSPC", "GC=F", "CL=F", "BTC-USD", "EURUSD=X"]
        
    def fetch_latest(self) -> Dict[str, float]:
        """Fetch latest prices for configured tickers."""
        # Import yfinance lazily so environments with incompatible versions don't crash at import time.
        # (Some yfinance versions require Python 3.10+ due to typing syntax.)
        try:
            import yfinance as yf  # type: ignore
        except Exception as e:
            print(
                "MarketData disabled: failed to import yfinance. "
                "If you're on Python 3.9, pin yfinance to a 3.9-compatible version or upgrade Python.\n"
                f"Import error: {e}"
            )
            return {}

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
