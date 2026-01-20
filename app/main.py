import time
import threading
import uvicorn
import json
from app.ingestion.rss import RSSIngestor
from app.storage.dedup import NewsStorage
from app.ai.relevance import RelevanceFilter
from app.ai.extract import EventExtractor
from app.dashboard.web import app as web_app

class NewsMonitorSystem:
    def __init__(self):
        self.ingestor = RSSIngestor()
        self.storage = NewsStorage()
        self.relevance_filter = None
        self.event_extractor = None
        
        try:
            self.relevance_filter = RelevanceFilter()
            self.event_extractor = EventExtractor()
        except ValueError as e:
            print(f"Warning: {e}. AI features disabled.")

    def run_api(self):
        print("Starting Dashboard API...", flush=True)
        uvicorn.run(web_app, host="0.0.0.0", port=8000, log_level="error")

    def run(self):
        # Start API in background thread
        api_thread = threading.Thread(target=self.run_api, daemon=True)
        api_thread.start()

        print("System booted and dashboard live at http://localhost:8000", flush=True)
        
        while True:
            self.process_cycle()
            time.sleep(60)

    def process_cycle(self):
        print("Fetching news...", flush=True)
        headlines = self.ingestor.fetch_headlines()
        
        # Phase 1: Ingest new items
        new_found = False
        for h in headlines:
            if not self.storage.exists(h):
                print(f"New headline fetched: {h}", flush=True)
                self.storage.save_headline(h, status="pending")
                new_found = True
        
        # Phase 2: Retrieve all pending items from storage to process
        # This handles items stuck from previous runs or crashes
        recent_items = self.storage.get_recent_news(limit=100)
        to_process = [item['title'] for item in recent_items if item.get('status') == 'pending']

        if not to_process:
            if new_found:
                print("All new items were ingested but somehow none are pending (unexpected).", flush=True)
            else:
                print("No new news.", flush=True)
            return

        print(f"Processing {len(to_process)} pending items...", flush=True)

        # Phase 3: Process the pending headlines (AI logic)
        for h in to_process:
            # AI Relevance filtering
            is_relevant = False
            if self.relevance_filter:
                print(f"Checking relevance: {h}", flush=True)
                self.storage.save_headline(h, status="analyzing")
                is_relevant = self.relevance_filter.is_relevant(h)
            
            event_data = None
            if is_relevant:
                print(f"✅ RELEVANT NEWS: {h}", flush=True)
                if self.event_extractor:
                    print(f"Extracting event data: {h}", flush=True)
                    self.storage.save_headline(h, status="extracting")
                    event_data = self.event_extractor.extract_event(h)
                    if event_data:
                        print(f"EXTRACTED JSON: {json.dumps(event_data)}", flush=True)
                
                self.storage.save_headline(h, status="relevant", event=event_data)
            else:
                print(f"❌ SKIPPED (Irrelevant): {h}", flush=True)
                self.storage.save_headline(h, status="ignored")

def main():
    storage = NewsStorage()
    for i in range(10):
        try:
            storage.client.ping()
            print("Connected to Redis successfully.")
            break
        except Exception:
            print(f"Waiting for Redis... (Attempt {i+1}/10)")
            time.sleep(2)
    else:
        print("Could not connect to Redis. Exiting.")
        return

    system = NewsMonitorSystem()
    system.run()

if __name__ == "__main__":
    main()
