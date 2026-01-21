import time
from app.storage.dedup import NewsStorage
from app.ai.relevance import RelevanceFilter

def run_relevance_worker():
    storage = NewsStorage()
    relevance_filter = None
    
    try:
        relevance_filter = RelevanceFilter()
    except Exception as e:
        print(f"Filter Init Error: {e}")
        return

    print("Relevance Worker started...")

    idle_seconds = 0
    while True:
        try:
            tasks = storage.pop_batch_from_queue("relevance", batch_size=5)
            if not tasks:
                idle_seconds += 2
                if idle_seconds >= 60:
                    print(f"[{time.ctime()}] Relevance Worker Heartbeat: Waiting for tasks...", flush=True)
                    idle_seconds = 0
                time.sleep(2)
                continue
            
            idle_seconds = 0
            headlines = [t['title'] for t in tasks]
            for h in headlines:
                storage.save_headline(h, status="analyzing")
                print(f"Status: ANALYZING - {h}", flush=True)

            if headlines:
                print(f"Processing batch of {len(headlines)} relevance checks...", flush=True)
                results = relevance_filter.is_relevant_batch(headlines)
                
                for i, is_relevant in enumerate(results):
                    h = headlines[i]
                    if is_relevant:
                        storage.save_headline(h, status="extracting")
                        print(f"Status: EXTRACTING - {h}", flush=True)
                        storage.push_to_queue("extraction", {"title": h})
                    else:
                        storage.save_headline(h, status="ignored")
                        print(f"Status: IGNORED - {h}", flush=True)
                
        except Exception as e:
            print(f"Relevance Worker Error: {e}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    run_relevance_worker()
