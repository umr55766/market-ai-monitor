import sqlite3
import json
import os
from typing import List, Optional

class DashboardDB:
    def __init__(self, db_path: str = "data/market_monitor.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
                    hash TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    event_data TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON news(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON news(timestamp DESC)")

    def save_news(self, news_hash: str, title: str, status: str, timestamp: float, event: Optional[dict] = None):
        event_json = json.dumps(event) if event else None
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO news (hash, title, status, timestamp, event_data)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(hash) DO UPDATE SET
                    status = excluded.status,
                    event_data = COALESCE(excluded.event_data, news.event_data)
            """, (news_hash, title, status, timestamp, event_json))

    def exists(self, news_hash: str) -> bool:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT 1 FROM news WHERE hash = ?", (news_hash,))
            return cursor.fetchone() is not None

    def get_recent(self, limit: int = 100) -> List[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT title, status, timestamp, event_data 
                FROM news 
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                item = {
                    "title": row["title"],
                    "status": row["status"],
                    "timestamp": row["timestamp"],
                    "event": json.loads(row["event_data"]) if row["event_data"] else None
                }
                results.append(item)
            return results

    def get_pending_hashes(self, limit: int = 500) -> List[str]:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT hash FROM news WHERE status = 'pending' LIMIT ?", (limit,))
            return [row["hash"] for row in cursor.fetchall()]

    def get_news_by_hash(self, news_hash: str) -> Optional[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT * FROM news WHERE hash = ?", (news_hash,))
            row = cursor.fetchone()
            if row:
                return {
                    "title": row["title"],
                    "status": row["status"],
                    "timestamp": row["timestamp"],
                    "event": json.loads(row["event_data"]) if row["event_data"] else None
                }
        return None
