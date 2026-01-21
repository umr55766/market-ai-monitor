import time
from app.storage.dedup import NewsStorage
from app.ai.relevance import RelevanceFilter
from app.runtime import heartbeat_sleep

def run_relevance_worker():
    BATCH_SIZE = 5
    IDLE_POLL_S = 2
    HEARTBEAT_EVERY_S = 60

    storage = NewsStorage()
    relevance_filter = None
    
    try:
        relevance_filter = RelevanceFilter()
    except Exception as e:
        print(f"Filter Init Error: {e}")
        return

    print("Relevance Worker started...")

    while True:
        try:
            tasks = storage.pop_batch_from_queue("relevance", batch_size=BATCH_SIZE)
            if not tasks:
                heartbeat_sleep(
                    sleep_s=IDLE_POLL_S,
                    heartbeat_every_s=HEARTBEAT_EVERY_S,
                    heartbeat=lambda: print(
                        f"[{time.ctime()}] Relevance Worker Heartbeat: Waiting for tasks...",
                        flush=True,
                    ),
                    tick_s=IDLE_POLL_S,
                )
                continue
            
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
