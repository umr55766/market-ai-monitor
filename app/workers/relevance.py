import time
from app.storage.dedup import NewsStorage
from app.ai.relevance import RelevanceFilter

def run_relevance_worker():
    storage = NewsStorage()
    relevance_filter = None
    
    try:
        relevance_filter = RelevanceFilter()
    except Exception as e:
        print(f"Filter Init Error: {e}")
        return

    print("Relevance Worker started...")

    while True:
        try:
            task = storage.pop_from_queue("relevance")
            if not task:
                continue
                
            headline = task['title']
            print(f"Analyzing relevance: {headline}", flush=True)
            
            storage.save_headline(headline, status="analyzing")
            is_relevant = relevance_filter.is_relevant(headline)
            
            if is_relevant:
                print(f"✅ Relevant: {headline}", flush=True)
                storage.save_headline(headline, status="extracting")
                storage.push_to_queue("extraction", {"title": headline})
            else:
                print(f"❌ Ignored: {headline}", flush=True)
                storage.save_headline(headline, status="ignored")
                
        except Exception as e:
            print(f"Relevance Worker Error: {e}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    run_relevance_worker()
