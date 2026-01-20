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

    while True:
        try:
            task = storage.pop_from_queue("extraction")
            if not task:
                continue
                
            headline = task['title']
            print(f"Extracting event: {headline}", flush=True)
            
            event_data = extractor.extract_event(headline)
            if event_data:
                print(f"EXTRACTED DATA: {json.dumps(event_data)}", flush=True)
            
            storage.save_headline(headline, status="relevant", event=event_data)
            
        except Exception as e:
            print(f"Extraction Worker Error: {e}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    run_extraction_worker()
