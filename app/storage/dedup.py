import redis
from typing import Optional, List
import hashlib
import os
import json
import time

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

    def _get_hash(self, text: str) -> str:
        return hashlib.sha256(text.encode('utf-8')).hexdigest()

    def exists(self, headline: str) -> bool:
        headline_hash = self._get_hash(headline)
        return self.client.exists(f"news:{headline_hash}") == 1

    def save_headline(self, title: str, status: str, event: dict = None):
        headline_hash = self._get_hash(title)
        key = f"news:{headline_hash}"
        
        
        existing_data = self.client.get(key)
        timestamp = None
        if existing_data:
            try:
                old_meta = json.loads(existing_data)
                timestamp = old_meta.get('timestamp')
                
                if event is None and status != "pending":
                    event = old_meta.get('event')
            except:
                pass
        
        is_update = existing_data is not None
        metadata = NewsMetadata(title, status=status, timestamp=timestamp, event=event)
        data = json.dumps(metadata.to_dict())
        
        self.client.setex(key, 86400 * 7, data)
        
        if not is_update:
            self.client.lpush("recent_news", headline_hash)
            self.client.ltrim("recent_news", 0, 499)

    def get_recent_news(self, limit: int = 100):
        hashes = self.client.lrange("recent_news", 0, limit - 1)
        news_items = []
        for h in hashes:
            data = self.client.get(f"news:{h.decode('utf-8')}")
            if data:
                news_items.append(json.loads(data))
        return news_items

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
        hashes = self.client.lrange("recent_news", 0, 499)
        requeued_count = 0
        for h in hashes:
            data = self.client.get(f"news:{h.decode('utf-8')}")
            if data:
                item = json.loads(data)
                if item.get('status') == 'pending':
                    self.push_to_queue("relevance", {"title": item['title']})
                    requeued_count += 1
        return requeued_count
