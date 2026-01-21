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
            
            # Auto-recovery: Requeue any items that were stuck in pending/analyzing/extracting
            requeued = storage.requeue_pending()
            if requeued > 0:
                print(f"  [RECOVERY] Requeued {requeued} stuck tasks.", flush=True)

            entries = ingestor.fetch_headlines()
            new_count = 0
            skipped_old = 0
            current_time = time.time()
            one_day_ago = current_time - 86400  # 24 hours in seconds
            
            for entry in entries:
                h = entry['title']
                link = entry['link']
                published = entry.get('published')
                
                # Skip news older than 1 day
                if published and published < one_day_ago:
                    skipped_old += 1
                    continue
                
                if not storage.exists(h):
                    new_count += 1
                    print(f"  [NEW] {h}", flush=True)
                    storage.save_headline(h, status="pending", link=link, published=published)
                    storage.push_to_queue("relevance", {"title": h})
                else:
                    # Update publication date for existing entries if we have it
                    if published:
                        existing = storage.db.get_news_by_hash(storage._get_hash(h))
                        if existing and abs(existing['timestamp'] - published) > 86400:  # More than 1 day difference
                            print(f"  [BACKFILL] Updating timestamp for: {h[:60]}...", flush=True)
                            storage.save_headline(h, status=existing['status'], link=link, published=published, event=existing.get('event'))
            
            if skipped_old > 0:
                print(f"  [FILTERED] Skipped {skipped_old} articles older than 1 day", flush=True)
            print(f"--- Fetch Cycle Finished. Total: {len(entries)} items, New: {new_count} ---", flush=True)
            print(f"Sleeping for 120s...", flush=True)
            time.sleep(120)
        except Exception as e:
            print(f"Ingestor Error: {e}", flush=True)
            time.sleep(10)

if __name__ == "__main__":
    run_ingestor()
