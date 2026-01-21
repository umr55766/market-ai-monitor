import feedparser
import os
import requests
from typing import List
from app.ingestion.base import NewsSource
from app.ingestion.schema_learner import FeedSchemaLearner

class RSSIngestor(NewsSource):
    def __init__(self):
        self.feeds = self._get_feeds()
        self.schema_learner = FeedSchemaLearner()

    def _get_feeds(self) -> List[str]:
        feeds_str = os.getenv("RSS_FEEDS", "")
        if not feeds_str:
            return []
        return [url.strip() for url in feeds_str.split(",") if url.strip()]

    def fetch_headlines(self) -> List[dict]:
        results = []
        for url in self.feeds:
            try:
                print(f"Fetching feed: {url}", flush=True)
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                feed = feedparser.parse(response.content)
                
                # Learn schema directly from feed response entries
                if feed.entries:
                    schema = self.schema_learner.learn_schema(url, feed.entries)
                else:
                    schema = self.schema_learner._default_schema()
                
                # Parse all entries using the schema
                for entry in feed.entries:
                    if hasattr(entry, 'title'):
                        parsed = self.schema_learner.parse_entry(entry, schema)
                        
                        results.append({
                            "title": parsed["title"] or entry.title,
                            "link": parsed["link"],
                            "published": parsed["published"]
                        })
                
                print(f"Success: Found {len(feed.entries)} items from {url}", flush=True)
            except Exception as e:
                print(f"Error fetching {url}: {e}", flush=True)
        return results
