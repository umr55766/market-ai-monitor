from app.ai.extract import EventExtractor
import os

def test_extraction():
    extractor = EventExtractor()
    headline = "Federal Reserve signals potential rate hike in upcoming meeting as inflation persists"
    print(f"Testing headline: {headline}")
    event = extractor.extract_event(headline)
    if event:
        print("Successfully extracted event data:")
        print(event)
    else:
        print("Extraction failed.")

if __name__ == "__main__":
    test_extraction()
