import uvicorn
import time
from app.storage.dedup import NewsStorage
from app.dashboard.web import app as web_app

def main():
    # Wait for Redis to be ready
    storage = NewsStorage()
    for i in range(10):
        try:
            storage.client.ping()
            print("Connected to Redis successfully.")
            break
        except Exception:
            print(f"Waiting for Redis... (Attempt {i+1}/10)")
            time.sleep(2)
    else:
        print("Could not connect to Redis. Exiting.")
        return

    print("Starting Dashboard API...", flush=True)
    uvicorn.run(web_app, host="0.0.0.0", port=8000, log_level="error")

if __name__ == "__main__":
    main()
