import feedparser
import os
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

    def fetch_headlines(self) -> List[str]:
        headlines = []
        for url in self.feeds:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries:
                    headlines.append(entry.title)
            except Exception as e:
                print(f"Error fetching {url}: {e}")
        return headlines
