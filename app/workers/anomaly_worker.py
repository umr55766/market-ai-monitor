import time

from app.ai.narrate import AlertNarrator
from app.alerts.scoring import SeverityScorer
from app.alerts.telegram import TelegramBot
from app.market.anomalies import AnomalyDetector
from app.storage.dedup import NewsStorage


def run_anomaly_worker():
    storage = NewsStorage()
    detector = AnomalyDetector(storage.db, threshold=0.005) # 0.5% for testing
    scorer = SeverityScorer()
    
    try:
        narrator = AlertNarrator()
        telegram = TelegramBot()
        alerts_enabled = True
        print("✓ Telegram alerts enabled", flush=True)
    except Exception as e:
        print(f"⚠ Telegram alerts disabled: {e}", flush=True)
        alerts_enabled = False
    
    print("Anomaly Detection Worker started (Polling every 60s)...", flush=True)
    
    sent_alerts = set()
    
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
                
                if level in ["HIGH", "CRITICAL"] and alerts_enabled:
                    alert_key = f"{anomaly['ticker']}_{int(anomaly['timestamp'])}"
                    
                    if alert_key not in sent_alerts:
                        print(f"  !!! Generating alert for {anomaly['ticker']} !!!", flush=True)
                        
                        narrative = narrator.narrate_alert(anomaly, correlations)
                        
                        success = telegram.send_alert(
                            anomaly['ticker'],
                            anomaly['change_pct'],
                            level,
                            narrative
                        )
                        
                        if success:
                            sent_alerts.add(alert_key)
                            if len(sent_alerts) > 100:
                                sent_alerts.pop()
                    
        except Exception as e:
            print(f"Anomaly Worker Error: {e}", flush=True)
            time.sleep(10)

if __name__ == "__main__":
    run_anomaly_worker()
