from google import genai
import feedparser
import time
import os
import json
from typing import Optional, Dict, List

class FeedSchemaLearner:
    """
    AI-powered RSS feed schema learner that analyzes feed structures
    directly from RSS feed responses.
    """
    
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        # In-memory cache to avoid re-learning on every request
        self._schema_cache: Dict[str, Dict] = {}
        
        if self.api_key:
            self.client = genai.Client(api_key=self.api_key)
            self.model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash-exp")
            self.ai_enabled = True
        else:
            self.ai_enabled = False
            print("⚠ Feed schema learning disabled: GEMINI_API_KEY not set", flush=True)
    
    def learn_schema(self, feed_url: str, feed_entries: List[feedparser.FeedParserDict]) -> Dict:
        """
        Use AI to analyze RSS feed entries and determine the optimal parsing schema.
        
        Args:
            feed_url: The RSS feed URL
            feed_entries: List of entries from the feed response
        
        Returns:
            Schema dict with field mappings
        """
        # Check in-memory cache first
        if feed_url in self._schema_cache:
            return self._schema_cache[feed_url]
        
        if not self.ai_enabled or not feed_entries:
            return self._default_schema()
        
        # Extract sample data from multiple entries to get a better understanding
        sample_entries = []
        for entry in feed_entries[:3]:  # Analyze up to 3 entries
            sample_data = {}
            for key in entry.keys():
                val = entry[key]
                # Only include simple types for the sample
                if isinstance(val, (str, int, float, bool)):
                    sample_data[key] = str(val)[:200]  # Truncate long values
            sample_entries.append(sample_data)
        
        prompt = f"""Analyze these RSS feed entries and determine the best field mappings for parsing.

Feed URL: {feed_url}

Sample entries (showing structure across multiple entries):
{json.dumps(sample_entries, indent=2)}

Return a JSON object with field mappings in this exact format:
{{
  "title_field": "title",
  "link_field": "link",
  "date_fields": ["published", "updated", "pubDate"],
  "description_field": "summary",
  "author_field": "author"
}}

Rules:
- date_fields should be an array of field names to try in order of preference
- Use null for fields that don't exist
- Analyze all sample entries to find the most consistent field names
- Only return the JSON, nothing else"""

        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt
            )
            
            text = response.text.strip()
            # Remove markdown code blocks if present
            text = text.replace('```json', '').replace('```', '').strip()
            
            schema = json.loads(text)
            
            # Cache in memory
            self._schema_cache[feed_url] = schema
            
            print(f"✓ Learned schema for {feed_url} from feed response", flush=True)
            return schema
            
        except Exception as e:
            print(f"⚠ Schema learning failed for {feed_url}: {e}", flush=True)
            return self._default_schema()
    
    def _default_schema(self) -> Dict:
        """Return a default schema for standard RSS feeds."""
        return {
            "title_field": "title",
            "link_field": "link",
            "date_fields": ["published", "updated", "pubDate"],
            "description_field": "summary",
            "author_field": "author"
        }
    
    def parse_entry(self, entry: feedparser.FeedParserDict, schema: Dict) -> Dict:
        """
        Parse an RSS entry using the provided schema.
        
        Args:
            entry: RSS entry from feedparser
            schema: Schema dict with field mappings
        
        Returns:
            Parsed entry dict
        """
        result = {
            "title": None,
            "link": None,
            "published": None,
            "description": None,
            "author": None
        }
        
        # Extract title
        title_field = schema.get("title_field")
        if title_field and hasattr(entry, title_field):
            result["title"] = getattr(entry, title_field)
        
        # Extract link
        link_field = schema.get("link_field")
        if link_field and hasattr(entry, link_field):
            result["link"] = getattr(entry, link_field)
        
        # Extract publication date (try multiple fields)
        date_fields = schema.get("date_fields", [])
        for field in date_fields:
            # Try parsed time struct first
            parsed_field = f"{field}_parsed"
            if hasattr(entry, parsed_field):
                try:
                    result["published"] = time.mktime(getattr(entry, parsed_field))
                    break
                except:
                    pass
            
            # Try raw string
            if hasattr(entry, field):
                try:
                    import email.utils
                    date_str = getattr(entry, field)
                    if date_str:
                        parsed = email.utils.parsedate_to_datetime(date_str)
                        result["published"] = parsed.timestamp()
                        break
                except:
                    pass
        
        # Extract description
        desc_field = schema.get("description_field")
        if desc_field and hasattr(entry, desc_field):
            result["description"] = getattr(entry, desc_field)
        
        # Extract author
        author_field = schema.get("author_field")
        if author_field and hasattr(entry, author_field):
            result["author"] = getattr(entry, author_field)
        
        return result
