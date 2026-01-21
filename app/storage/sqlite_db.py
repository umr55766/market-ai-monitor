import sqlite3
import json
import os
import time
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
                    link TEXT,
                    status TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    event_data TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON news(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON news(timestamp DESC)")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS market_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    price REAL NOT NULL,
                    timestamp REAL NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ticker_ts ON market_prices(ticker, timestamp DESC)")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS anomalies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    change_pct REAL NOT NULL,
                    score REAL NOT NULL,
                    level TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    correlations TEXT
                )
            """)

    def save_news(self, news_hash: str, title: str, status: str, timestamp: float, link: Optional[str] = None, event: Optional[dict] = None):
        event_json = json.dumps(event) if event else None
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO news (hash, title, link, status, timestamp, event_data)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(hash) DO UPDATE SET
                    link = COALESCE(excluded.link, news.link),
                    status = excluded.status,
                    event_data = COALESCE(excluded.event_data, news.event_data)
            """, (news_hash, title, link, status, timestamp, event_json))

    def exists(self, news_hash: str) -> bool:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT 1 FROM news WHERE hash = ?", (news_hash,))
            return cursor.fetchone() is not None

    def get_recent(self, limit: int = 100) -> List[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT title, link, status, timestamp, event_data 
                FROM news 
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                item = {
                    "title": row["title"],
                    "link": row["link"],
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

    def get_stuck_hashes(self, limit: int = 500) -> List[dict]:
        """Finds items that are in a non-final state."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT hash, status FROM news 
                WHERE status IN ('pending', 'analyzing', 'extracting') 
                LIMIT ?
            """, (limit,))
            return [{"hash": row["hash"], "status": row["status"]} for row in cursor.fetchall()]

    def get_news_by_hash(self, news_hash: str) -> Optional[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT * FROM news WHERE hash = ?", (news_hash,))
            row = cursor.fetchone()
            if row:
                return {
                    "title": row["title"],
                    "link": row["link"],
                    "status": row["status"],
                    "timestamp": row["timestamp"],
                    "event": json.loads(row["event_data"]) if row["event_data"] else None
                }
        return None

    def save_price(self, ticker: str, price: float):
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO market_prices (ticker, price, timestamp)
                VALUES (?, ?, ?)
            """, (ticker, price, time.time()))

    def get_latest_prices(self) -> dict:
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT ticker, price 
                FROM market_prices 
                WHERE id IN (SELECT MAX(id) FROM market_prices GROUP BY ticker)
            """)
            return {row["ticker"]: row["price"] for row in cursor.fetchall()}

    def save_anomaly(self, ticker: str, change_pct: float, score: float, level: str, correlations: List[dict]):
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO anomalies (ticker, change_pct, score, level, timestamp, correlations)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ticker, change_pct, score, level, time.time(), json.dumps(correlations)))

    def get_recent_anomalies(self, limit: int = 10) -> List[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM anomalies 
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            return [
                {
                    "ticker": row["ticker"],
                    "change_pct": row["change_pct"],
                    "score": row["score"],
                    "level": row["level"],
                    "timestamp": row["timestamp"],
                    "correlations": json.loads(row["correlations"])
                } for row in rows
            ]

    def get_price_history(self, ticker: str, limit: int = 20) -> List[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT price, timestamp 
                FROM market_prices 
                WHERE ticker = ? 
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (ticker, limit))
            return [{"price": row["price"], "timestamp": row["timestamp"]} for row in cursor.fetchall()]
