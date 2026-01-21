import feedparser
import os
import requests
import time
from typing import List
from app.ingestion.base import NewsSource

class RSSIngestor(NewsSource):
    def __init__(self):
        self.feeds = self._get_feeds()

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
                for entry in feed.entries:
                    if hasattr(entry, 'title'):
                        # Extract published timestamp
                        pub_timestamp = None
                        if hasattr(entry, 'published_parsed') and entry.published_parsed:
                            pub_timestamp = time.mktime(entry.published_parsed)
                        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                            pub_timestamp = time.mktime(entry.updated_parsed)
                        
                        results.append({
                            "title": entry.title,
                            "link": getattr(entry, 'link', None),
                            "published": pub_timestamp
                        })
                print(f"Success: Found {len(feed.entries)} items from {url}", flush=True)
            except Exception as e:
                print(f"Error fetching {url}: {e}", flush=True)
        return results
