from typing import List, Optional

class SeverityScorer:
    @staticmethod
    def calculate_score(anomaly: dict, correlations: List[dict]) -> float:
        """
        Calculates a severity score (0-100) based on market move and news alignment.
        """
        # Base score from market move magnitude
        # 1% move = 20 points, 5% move = 100 points
        move_score = min(abs(anomaly['change_pct']) * 20, 60)
        
        # News alignment score
        news_score = 0
        if correlations:
            # More correlations = higher confidence
            news_score += min(len(correlations) * 15, 30)
            
            # AI certainty bonus (look at the first correlation)
            top_news = correlations[0]
            if top_news.get('event', {}).get('certainty', 0) > 0.8:
                news_score += 10
        
        return min(move_score + news_score, 100)

    @staticmethod
    def get_level(score: float) -> str:
        if score >= 80: return "CRITICAL"
        if score >= 50: return "HIGH"
        if score >= 25: return "MEDIUM"
        return "LOW"
