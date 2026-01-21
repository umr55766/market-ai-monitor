import time
import json
from app.storage.dedup import NewsStorage
from app.ai.extract import EventExtractor

def run_extraction_worker():
    storage = NewsStorage()
    extractor = None
    
    try:
        extractor = EventExtractor()
    except Exception as e:
        print(f"Extractor Init Error: {e}")
        return

    print("Extraction Worker started...")

    idle_seconds = 0
    while True:
        try:
            tasks = storage.pop_batch_from_queue("extraction", batch_size=3)
            if not tasks:
                idle_seconds += 2
                if idle_seconds >= 60:
                    print(f"[{time.ctime()}] Extraction Worker Heartbeat: Waiting for tasks...", flush=True)
                    idle_seconds = 0
                time.sleep(2)
                continue
            
            idle_seconds = 0
            headlines = [t['title'] for t in tasks]
            for h in headlines:
                print(f"Status: EXTRACTING - {h}", flush=True)

            if headlines:
                print(f"Processing batch of {len(headlines)} extractions...", flush=True)
                batch_data = extractor.extract_events_batch(headlines)
                
                for i, event_data in enumerate(batch_data):
                    headline = headlines[i]
                    if event_data:
                        print(f"EXTRACTED DATA for '{headline}': {json.dumps(event_data)}", flush=True)
                    
                    storage.save_headline(headline, status="relevant", event=event_data)
                    print(f"Status: RELEVANT - {headline}", flush=True)
                
        except Exception as e:
            print(f"Extraction Worker Error: {e}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    run_extraction_worker()
