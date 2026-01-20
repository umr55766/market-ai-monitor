import redis
import hashlib
import os
import json
import time

class NewsMetadata:
    def __init__(self, title: str, status: str = "pending", timestamp: float = None, event: dict = None):
        self.title = title
        self.status = status # pending, relevant, ignored
        self.timestamp = timestamp or time.time()
        self.event = event

    def to_dict(self):
        return {
            "title": self.title,
            "status": self.status,
            "timestamp": self.timestamp,
            "event": self.event
        }

class NewsStorage:
    def __init__(self, redis_host: str = None, redis_port: int = 6379):
        if redis_host is None:
            redis_host = os.getenv("REDIS_HOST", "localhost")
        
        # Add retry strategy for startup stability
        self.client = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            db=0,
            retry_on_timeout=True,
            health_check_interval=30
        )

    def _get_hash(self, text: str) -> str:
        return hashlib.sha256(text.encode('utf-8')).hexdigest()

    def exists(self, headline: str) -> bool:
        headline_hash = self._get_hash(headline)
        return self.client.exists(f"news:{headline_hash}") == 1

    def save_headline(self, title: str, status: str, event: dict = None):
        headline_hash = self._get_hash(title)
        
        # Check if we are updating an existing one to avoid duplicate list entries
        is_update = self.client.exists(f"news:{headline_hash}")
        
        metadata = NewsMetadata(title, status=status, event=event)
        data = json.dumps(metadata.to_dict())
        
        # Save metadata (expires in 7 days)
        self.client.setex(f"news:{headline_hash}", 86400 * 7, data)
        
        if not is_update:
            # Add to recent list only for new entries (limit to 100)
            self.client.lpush("recent_news", headline_hash)
            self.client.ltrim("recent_news", 0, 99)

    def get_recent_news(self, limit: int = 50):
        hashes = self.client.lrange("recent_news", 0, limit - 1)
        news_items = []
        for h in hashes:
            data = self.client.get(f"news:{h.decode('utf-8')}")
            if data:
                news_items.append(json.loads(data))
        return news_items
