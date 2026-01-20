import redis
import hashlib
import os

class DeduplicationService:
    def __init__(self, redis_host: str = None, redis_port: int = 6379):
        if redis_host is None:
            redis_host = os.getenv("REDIS_HOST", "localhost")
        self.client = redis.Redis(host=redis_host, port=redis_port, db=0)

    def _get_hash(self, text: str) -> str:
        return hashlib.sha256(text.encode('utf-8')).hexdigest()

    def exists(self, headline: str) -> bool:
        """Checks if the headline exists in the cache without modifying state."""
        headline_hash = self._get_hash(headline)
        return self.client.exists(f"news:{headline_hash}") == 1

    def add(self, headline: str, ttl: int = 86400) -> None:
        """Adds the headline to the cache."""
        headline_hash = self._get_hash(headline)
        self.client.setex(f"news:{headline_hash}", ttl, headline)

    def is_new(self, headline: str) -> bool:
        """
        Orchestrates the check and set operation.
        Returns True if the item was not present and has now been added.
        """
        if self.exists(headline):
            return False
        
        self.add(headline)
        return True
