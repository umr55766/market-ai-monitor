import uvicorn
import time
from app.storage.dedup import NewsStorage
from app.dashboard.web import app as web_app
from app.runtime import wait_for

def main():
    storage = NewsStorage()
    ok = wait_for(
        lambda: bool(storage.client.ping()),
        attempts=10,
        delay_s=2,
        on_retry=lambda i, e: print(f"Waiting for Redis... (Attempt {i}/10)", flush=True),
    )
    if not ok:
        print("Could not connect to Redis. Exiting.", flush=True)
        return

    print("Starting Dashboard API...", flush=True)
    uvicorn.run(web_app, host="0.0.0.0", port=8000, log_level="error")

if __name__ == "__main__":
    main()
