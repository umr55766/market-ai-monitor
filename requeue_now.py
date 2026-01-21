from app.storage.dedup import NewsStorage

def manual_requeue():
    storage = NewsStorage()
    count = storage.requeue_pending()
    print(f"Requeued {count} pending items to the relevance queue.")

if __name__ == "__main__":
    manual_requeue()
