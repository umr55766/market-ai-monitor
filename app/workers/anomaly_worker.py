import time
import json
from app.market.anomalies import AnomalyDetector
from app.alerts.scoring import SeverityScorer
from app.storage.dedup import NewsStorage

def run_anomaly_worker():
    storage = NewsStorage()
    detector = AnomalyDetector(storage.db, threshold=0.005) # 0.5% for testing
    scorer = SeverityScorer()
    
    print("Anomaly Detection Worker started (Polling every 60s)...", flush=True)
    
    while True:
        try:
            time.sleep(60) # Run slightly after market worker
            print(f"--- Anomaly Check Started at {time.ctime()} ---", flush=True)
            
            anomalies = detector.detect_anomalies()
            for anomaly in anomalies:
                correlations = detector.correlate_with_news(anomaly)
                score = scorer.calculate_score(anomaly, correlations)
                level = scorer.get_level(score)
                
                print(f"  [ANOMALY] {anomaly['ticker']} {anomaly['change_pct']:.2f}% | Score: {score} ({level})", flush=True)
                
                storage.db.save_anomaly(
                    anomaly['ticker'], 
                    anomaly['change_pct'], 
                    score, 
                    level, 
                    correlations
                )
                
                if level in ["HIGH", "CRITICAL"]:
                    print(f"  !!! ALERT !!! High Severity Anomaly detected for {anomaly['ticker']}", flush=True)
                    # TODO: Store alert in DB or send to Telegram
                    
        except Exception as e:
            print(f"Anomaly Worker Error: {e}", flush=True)
            time.sleep(10)

if __name__ == "__main__":
    run_anomaly_worker()
