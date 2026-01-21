# market-ai-monitor

A market monitoring system that ingests RSS feeds, extracts market-relevant events using AI, and detects anomalies in stock prices.

## Setup

1. Copy `.env.template` to `.env` and configure your environment variables:
   ```bash
   cp .env.template .env
   ```

2. Set your RSS feeds (comma-separated):
   ```bash
   export RSS_FEEDS="https://rss.nytimes.com/services/xml/rss/nyt/Business.xml"
   ```

3. (Optional) Set GEMINI_API_KEY for AI-powered schema learning:
   ```bash
   export GEMINI_API_KEY="your_api_key"
   ```

## Running with Docker Compose

```bash
docker-compose up
```

## RSS Feed Schema Learning

The system automatically learns RSS feed schemas from feed responses using AI. If `GEMINI_API_KEY` is not set, it falls back to a default schema that works for most standard RSS feeds.

The schema learner analyzes feed entries to determine:
- Title field mapping
- Link field mapping  
- Date field mappings (tries multiple fields)
- Description/summary field mapping
- Author field mapping