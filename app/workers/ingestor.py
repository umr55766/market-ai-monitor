import time
from app.ingestion.rss import RSSIngestor
from app.storage.dedup import NewsStorage

def run_ingestor():
    ingestor = RSSIngestor()
    storage = NewsStorage()
    
    for i in range(5):
        try:
            storage.client.ping()
            break
        except:
            time.sleep(2)

    print("Ingestor Worker started...")
    
    while True:
        try:
            print(f"--- Fetch Cycle Started at {time.ctime()} ---", flush=True)
            headlines = ingestor.fetch_headlines()
            new_count = 0
            
            for h in headlines:
                if not storage.exists(h):
                    new_count += 1
                    print(f"  [NEW] {h}", flush=True)
                    storage.save_headline(h, status="pending")
                    storage.push_to_queue("relevance", {"title": h})
            
            print(f"--- Fetch Cycle Finished. Total: {len(headlines)} items, New: {new_count} ---", flush=True)
            print(f"Sleeping for 120s...", flush=True)
            time.sleep(120)
        except Exception as e:
            print(f"Ingestor Error: {e}", flush=True)
            time.sleep(10)

if __name__ == "__main__":
    run_ingestor()
