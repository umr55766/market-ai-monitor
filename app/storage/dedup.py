import redis
from typing import Optional, List
import hashlib
import os
import json
import time

from app.storage.sqlite_db import DashboardDB

class NewsMetadata:
    def __init__(self, title: str, status: str = "pending", timestamp: float = None, event: dict = None):
        self.title = title
        self.status = status
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
        
        self.client = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            db=0,
            retry_on_timeout=True,
            health_check_interval=30
        )
        self.db = DashboardDB()

    def _get_hash(self, text: str) -> str:
        return hashlib.sha256(text.encode('utf-8')).hexdigest()

    def exists(self, headline: str) -> bool:
        return self.db.exists(self._get_hash(headline))

    def save_headline(self, title: str, status: str, event: dict = None):
        h = self._get_hash(title)
        
        existing = self.db.get_news_by_hash(h)
        timestamp = existing['timestamp'] if existing else time.time()
        
        if event is None and status != "pending" and existing:
            event = existing.get('event')
            
        self.db.save_news(h, title, status, timestamp, event)

    def get_recent_news(self, limit: int = 100):
        return self.db.get_recent(limit)

    def push_to_queue(self, queue_name: str, data: dict):
        self.client.lpush(f"queue:{queue_name}", json.dumps(data))

    def pop_from_queue(self, queue_name: str, timeout: int = 5):
        result = self.client.brpop(f"queue:{queue_name}", timeout=timeout)
        if result:
            return json.loads(result[1].decode('utf-8'))
        return None

    def get_queue_length(self, queue_name: str) -> int:
        return self.client.llen(f"queue:{queue_name}")

    def pop_batch_from_queue(self, queue_name: str, batch_size: int = 5) -> List[dict]:
        pipe = self.client.pipeline()
        for _ in range(batch_size):
            pipe.rpop(f"queue:{queue_name}")
        
        results = pipe.execute()
        items = []
        for r in results:
            if r:
                items.append(json.loads(r.decode('utf-8')))
        return items

    def requeue_pending(self):
        hashes = self.db.get_pending_hashes()
        requeued_count = 0
        for h in hashes:
            item = self.db.get_news_by_hash(h)
            if item:
                self.push_to_queue("relevance", {"title": item['title']})
                requeued_count += 1
        return requeued_count
