import time
from app.ingestion.rss import RSSIngestor
from app.storage.dedup import DeduplicationService

class NewsMonitorSystem:
    def __init__(self):
        self.ingestor = RSSIngestor()
        self.deduplicator = DeduplicationService()

    def run(self):
        print("System booted", flush=True)
        while True:
            self.process_cycle()
            time.sleep(60)

    def process_cycle(self):
        print("Fetching news...", flush=True)
        headlines = self.ingestor.fetch_headlines()
        new_count = 0
        for h in headlines:
            if self.deduplicator.is_new(h):
                print(f"NEWS: {h}", flush=True)
                new_count += 1
        
        if new_count == 0:
            print("No new news.", flush=True)

def main():
    system = NewsMonitorSystem()
    system.run()

if __name__ == "__main__":
    main()
