import time
from app.ingestion.rss import RSSIngestor
from app.storage.dedup import NewsStorage

def run_ingestor():
    ingestor = RSSIngestor()
    storage = NewsStorage()
    
    # Wait for Redis
    for i in range(5):
        try:
            storage.client.ping()
            break
        except:
            time.sleep(2)

    print("Ingestor Worker started...")
    
    while True:
        try:
            print("Fetching news...", flush=True)
            headlines = ingestor.fetch_headlines()
            
            for h in headlines:
                if not storage.exists(h):
                    print(f"New headline: {h}", flush=True)
                    storage.save_headline(h, status="pending")
                    storage.push_to_queue("relevance", {"title": h})
            
            time.sleep(120) # Fetch every 2 mins
        except Exception as e:
            print(f"Ingestor Error: {e}", flush=True)
            time.sleep(10)

if __name__ == "__main__":
    run_ingestor()
