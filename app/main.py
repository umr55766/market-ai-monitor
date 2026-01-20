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
        new_count = 0
        for h in headlines:
            if not self.storage.exists(h):
                print(f"Checking relevance: {h}", flush=True)
                
                is_relevant = True
                if self.relevance_filter:
                    is_relevant = self.relevance_filter.is_relevant(h)
                
                event_data = None
                if is_relevant and self.event_extractor:
                    print(f"Extracting event data: {h}", flush=True)
                    event_data = self.event_extractor.extract_event(h)
                    if event_data:
                        print(f"EXTRACTED JSON: {json.dumps(event_data)}", flush=True)
                
                # Save to persistent storage with relevance status and event data
                self.storage.save_headline(h, is_relevant, event=event_data)
                
                if is_relevant:
                    print(f"✅ RELEVANT NEWS: {h}", flush=True)
                else:
                    print(f"❌ SKIPPED (Irrelevant): {h}", flush=True)
                
                new_count += 1
        
        if new_count == 0:
            print("No new news.", flush=True)

def main():
    system = NewsMonitorSystem()
    system.run()

if __name__ == "__main__":
    main()
