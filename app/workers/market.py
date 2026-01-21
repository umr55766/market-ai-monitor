import time
from app.market.prices import MarketData
from app.storage.dedup import NewsStorage

def run_market_worker():
    storage = NewsStorage()
    market = MarketData()
    
    print("Market Data Worker started (Polling every 60s)...", flush=True)
    
    while True:
        try:
            print(f"--- Market Fetch Started at {time.ctime()} ---", flush=True)
            prices = market.fetch_latest()
            
            for ticker, price in prices.items():
                print(f"  [MARKET] {ticker}: {price}", flush=True)
                storage.db.save_price(ticker, price)
            
            time.sleep(60)
        except Exception as e:
            print(f"Market Worker Error: {e}", flush=True)
            time.sleep(10)

if __name__ == "__main__":
    run_market_worker()
