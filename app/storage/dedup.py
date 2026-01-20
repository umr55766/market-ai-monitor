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
        key = f"news:{headline_hash}"
        
        # Try to preserve original timestamp if it exists
        existing_data = self.client.get(key)
        timestamp = None
        if existing_data:
            try:
                old_meta = json.loads(existing_data)
                timestamp = old_meta.get('timestamp')
                # If we aren't passing a new event, preserve the old one
                if event is None:
                    event = old_meta.get('event')
            except:
                pass
        
        is_update = existing_data is not None
        metadata = NewsMetadata(title, status=status, timestamp=timestamp, event=event)
        data = json.dumps(metadata.to_dict())
        
        # Save metadata (expires in 7 days)
        self.client.setex(key, 86400 * 7, data)
        
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

    def push_to_queue(self, queue_name: str, data: dict):
        # Push to head
        self.client.lpush(f"queue:{queue_name}", json.dumps(data))

    def pop_from_queue(self, queue_name: str, timeout: int = 5):
        # Pop from head (LIFO) so newest news are processed first
        # This matches the dashboard order and feels more "live"
        result = self.client.blpop(f"queue:{queue_name}", timeout=timeout)
        if result:
            return json.loads(result[1].decode('utf-8'))
        return None

    def requeue_pending(self):
        """Scan for pending items and re-add them to the relevance queue if lost."""
        hashes = self.client.lrange("recent_news", 0, 99)
        requeued_count = 0
        for h in hashes:
            data = self.client.get(f"news:{h.decode('utf-8')}")
            if data:
                item = json.loads(data)
                # If still pending (and not already in queue - though duplicates are okayish)
                if item.get('status') == 'pending':
                    self.push_to_queue("relevance", {"title": item['title']})
                    requeued_count += 1
        return requeued_count
