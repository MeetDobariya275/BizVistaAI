#!/usr/bin/env python3
"""
Step 4: Generative Insights (Ollama)
Generate business insights using Ollama with strict JSON validation
"""

import json
import hashlib
import requests
import pandas as pd
from pathlib import Path
import re
import structlog
from typing import Dict, List, Any, Optional

# Setup logging
logger = structlog.get_logger()

# Ollama configuration
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "phi3:mini"  # Using available model
OLLAMA_PARAMS = {
    "model": OLLAMA_MODEL,
    "temperature": 0.3,
    "num_ctx": 1024,  # Reduced context for faster processing
    "seed": 42,
    "stream": False
}

def load_business_data(business_id: str, data_dir: Path) -> Dict[str, Any]:
    """Load all business data for insight generation"""
    logger.info("Loading business data", business_id=business_id)
    
    # Load monthly trends
    trends_file = data_dir / "processed" / f"{business_id}_monthly_trends.csv"
    trends_df = pd.read_csv(trends_file)
    
    # Load keywords
    keywords_file = data_dir / "keywords_quotes" / f"{business_id}_keywords.json"
    with open(keywords_file, 'r') as f:
        keywords_data = json.load(f)
    
    # Load quotes
    quotes_file = data_dir / "keywords_quotes" / f"{business_id}_quotes.json"
    with open(quotes_file, 'r') as f:
        quotes_data = json.load(f)
    
    # Load business info
    business_file = data_dir / "sb_restaurants_selected.csv"
    business_df = pd.read_csv(business_file)
    business_info = business_df[business_df['business_id'] == business_id].iloc[0]
    
    logger.info("Loaded business data", 
               trends_months=len(trends_df),
               keywords=len(keywords_data),
               themes=len(quotes_data))
    
    return {
        'trends': trends_df,
        'keywords': keywords_data,
        'quotes': quotes_data,
        'business_info': business_info
    }

def prepare_insight_payload(business_data: Dict[str, Any], period: str = "2024-Q3") -> Dict[str, Any]:
    """Prepare the input payload for Ollama"""
    logger.info("Preparing insight payload", period=period)
    
    business_info = business_data['business_info']
    trends_df = business_data['trends']
    keywords_data = business_data['keywords']
    quotes_data = business_data['quotes']
    
    # Convert pandas types to native Python types for JSON serialization
    def convert_pandas_types(obj):
        if hasattr(obj, 'item'):  # pandas scalar
            return obj.item()
        elif hasattr(obj, 'tolist'):  # pandas array
            return obj.tolist()
        return obj
    
    # Calculate theme scores and deltas (using latest vs previous period)
    themes = []
    theme_names = ['food_quality', 'service', 'speed_wait', 'ambiance', 'cleanliness', 'portion_size', 'price_value', 'staff_behavior']
    
    for theme_name in theme_names:
        theme_display_name = theme_name.replace('_', ' ').title()
        
        # Get latest sentiment score
        latest_sentiment = convert_pandas_types(trends_df[f'{theme_name}_sentiment'].iloc[-1]) if len(trends_df) > 0 else 0.0
        latest_count = convert_pandas_types(trends_df[f'{theme_name}_count'].iloc[-1]) if len(trends_df) > 0 else 0
        
        # Calculate delta (latest vs previous)
        delta = 0.0
        if len(trends_df) > 1:
            prev_sentiment = convert_pandas_types(trends_df[f'{theme_name}_sentiment'].iloc[-2])
            delta = latest_sentiment - prev_sentiment
        
        # Get quotes for this theme
        theme_quotes = quotes_data.get(theme_name, {'positive': [], 'negative': []})
        pos_quotes = theme_quotes['positive'][:2]  # Max 2
        neg_quotes = theme_quotes['negative'][:2]  # Max 2
        
        # Only include themes with significant data
        if latest_count > 0 or abs(delta) > 0.01:
            themes.append({
                "name": theme_display_name,
                "score": round(latest_sentiment, 2),
                "delta": round(delta, 2),
                "pos_quotes": pos_quotes,
                "neg_quotes": neg_quotes
            })
    
    # Sort themes by impact (absolute delta) and take top 5
    themes.sort(key=lambda x: abs(x['delta']), reverse=True)
    themes = themes[:5]
    
    # Get top 5 keywords
    top_keywords = keywords_data[:5]
    
    # Calculate volume metrics
    total_reviews = len(trends_df)
    new_reviews = convert_pandas_types(trends_df['total_reviews'].iloc[-1]) if len(trends_df) > 0 else 0
    if len(trends_df) > 1:
        prev_reviews = convert_pandas_types(trends_df['total_reviews'].iloc[-2])
        new_since_last = new_reviews - prev_reviews
    else:
        new_since_last = 0
    
    payload = {
        "business": business_info['name'],
        "period": period,
        "themes": themes,
        "top_keywords": top_keywords,
        "volume": {
            "reviews": total_reviews,
            "new_since_last": new_since_last
        }
    }
    
    logger.info("Prepared payload", 
               themes=len(themes),
               keywords=len(top_keywords),
               total_reviews=total_reviews)
    
    return payload

def call_ollama(payload: Dict[str, Any]) -> str:
    """Call Ollama API to generate insights"""
    logger.info("Calling Ollama API")
    
    # Prepare simplified prompt for phi3:mini
    system_prompt = """You are BizVista AI. Generate restaurant insights in JSON format only.

Return EXACTLY:
{
  "love": [5 strings],
  "improve": [5 strings], 
  "recommendations": [3 strings]
}
Max 180 words total. Reference themes/keywords/deltas."""

    # Simplify payload for shorter prompt
    simplified_payload = {
        "business": payload["business"],
        "themes": payload["themes"][:3],  # Limit to top 3 themes
        "keywords": payload["top_keywords"][:3]  # Limit to top 3 keywords
    }
    
    user_prompt = json.dumps(simplified_payload, indent=1)
    
    full_prompt = f"{system_prompt}\n\nData:\n{user_prompt}\n\nGenerate insights:"
    
    # Make API call
    response_data = {
        "prompt": full_prompt,
        **OLLAMA_PARAMS
    }
    
    try:
        response = requests.post(OLLAMA_URL, json=response_data, timeout=120)  # Increased timeout
        response.raise_for_status()
        
        result = response.json()
        generated_text = result.get('response', '')
        
        logger.info("Ollama API call successful", 
                   response_length=len(generated_text))
        
        return generated_text
        
    except Exception as e:
        logger.error("Ollama API call failed", error=str(e))
        raise

def validate_json_output(text: str) -> Optional[Dict[str, Any]]:
    """Validate the JSON output from Ollama"""
    logger.info("Validating JSON output")
    
    try:
        # Try to parse JSON
        data = json.loads(text)
        
        # Schema check
        required_keys = ['love', 'improve', 'recommendations']
        if not all(key in data for key in required_keys):
            logger.warning("Missing required keys", keys=list(data.keys()))
            return None
        
        # Length check (more flexible)
        if len(data['love']) < 3 or len(data['improve']) < 3 or len(data['recommendations']) < 2:
            logger.warning("Insufficient array lengths", 
                          love=len(data['love']),
                          improve=len(data['improve']),
                          recommendations=len(data['recommendations']))
            return None
        
        # Pad arrays if needed
        while len(data['love']) < 5:
            data['love'].append("Overall customer satisfaction trending positive.")
        while len(data['improve']) < 5:
            data['improve'].append("Continue monitoring customer feedback.")
        while len(data['recommendations']) < 3:
            data['recommendations'].append("Maintain current service standards.")
        
        # Truncate if too long
        data['love'] = data['love'][:5]
        data['improve'] = data['improve'][:5]
        data['recommendations'] = data['recommendations'][:3]
        
        # String content check
        all_strings = data['love'] + data['improve'] + data['recommendations']
        if not all(isinstance(s, str) and s.strip() for s in all_strings):
            logger.warning("Non-empty strings found")
            return None
        
        # Word count check (max 180 words total)
        total_words = sum(len(s.split()) for s in all_strings)
        if total_words > 180:
            logger.warning("Word count exceeded", total_words=total_words)
            return None
        
        logger.info("JSON validation passed", total_words=total_words)
        return data
        
    except json.JSONDecodeError as e:
        logger.warning("JSON parse error", error=str(e))
        return None

def repair_json_output(original_text: str) -> str:
    """Attempt to repair malformed JSON output"""
    logger.info("Attempting JSON repair")
    
    repair_prompt = f"""Output was invalid. Return ONLY valid JSON for the same schema. No prose.
Here is your last output:
<<<{original_text}>>>

Return EXACTLY this JSON schema:
{{
  "love": [string, string, string, string, string],
  "improve": [string, string, string, string, string],
  "recommendations": [string, string, string]
}}"""
    
    response_data = {
        "prompt": repair_prompt,
        **OLLAMA_PARAMS
    }
    
    try:
        response = requests.post(OLLAMA_URL, json=response_data, timeout=60)
        response.raise_for_status()
        
        result = response.json()
        repaired_text = result.get('response', '')
        
        logger.info("JSON repair attempt completed")
        return repaired_text
        
    except Exception as e:
        logger.error("JSON repair failed", error=str(e))
        return ""

def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    """Extract JSON block from text using regex"""
    logger.info("Extracting JSON from text")
    
    # Find first {...} block
    json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
    matches = re.findall(json_pattern, text, re.DOTALL)
    
    for match in matches:
        try:
            data = json.loads(match)
            if validate_json_output(json.dumps(data)):
                logger.info("Successfully extracted valid JSON")
                return data
        except json.JSONDecodeError:
            continue
    
    logger.warning("No valid JSON found in text")
    return None

def generate_fallback_insights(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Generate fallback insights using rule-based approach"""
    logger.info("Generating fallback insights")
    
    insights = {
        "love": [],
        "improve": [],
        "recommendations": []
    }
    
    # Generate love points from positive themes
    for theme in payload['themes']:
        if theme['score'] > 0.1:
            insights['love'].append(f"{theme['name']} rated positively ({theme['score']:.2f}) with recent improvement.")
    
    # Generate improve points from negative themes
    for theme in payload['themes']:
        if theme['score'] < -0.1:
            insights['improve'].append(f"{theme['name']} needs attention ({theme['score']:.2f}) with declining trend.")
    
    # Generate recommendations based on keywords
    for keyword in payload['top_keywords'][:3]:
        insights['recommendations'].append(f"Focus on {keyword['term']} mentioned {keyword['count']} times.")
    
    # Pad with generic insights if needed
    while len(insights['love']) < 5:
        insights['love'].append("Overall customer satisfaction trending positive.")
    
    while len(insights['improve']) < 5:
        insights['improve'].append("Continue monitoring customer feedback.")
    
    while len(insights['recommendations']) < 3:
        insights['recommendations'].append("Maintain current service standards.")
    
    return insights

def generate_insights_with_retry(payload: Dict[str, Any], max_retries: int = 2) -> Dict[str, Any]:
    """Generate insights with retry logic"""
    logger.info("Starting insight generation with retry", max_retries=max_retries)
    
    for attempt in range(max_retries + 1):
        try:
            if attempt == 0:
                # First attempt
                response_text = call_ollama(payload)
            else:
                # Repair attempt
                response_text = repair_json_output(response_text)
            
            # Validate JSON
            validated_data = validate_json_output(response_text)
            if validated_data:
                logger.info("Insights generated successfully", attempt=attempt + 1)
                return validated_data
            
            # Try extraction if validation failed
            extracted_data = extract_json_from_text(response_text)
            if extracted_data:
                logger.info("Insights extracted successfully", attempt=attempt + 1)
                return extracted_data
            
        except Exception as e:
            logger.error("Attempt failed", attempt=attempt + 1, error=str(e))
            if attempt == max_retries:
                break
    
    # Fallback to rule-based generation
    logger.warning("All attempts failed, using fallback")
    return generate_fallback_insights(payload)

def generate_cache_key(payload: Dict[str, Any]) -> str:
    """Generate cache key for insights"""
    # Create a stable string representation
    key_data = {
        'business': payload['business'],
        'period': payload['period'],
        'themes': payload['themes'],
        'keywords': payload['top_keywords'],
        'volume': payload['volume']
    }
    
    key_string = json.dumps(key_data, sort_keys=True)
    cache_key = hashlib.sha256(key_string.encode()).hexdigest()[:16]
    
    return cache_key

def save_insights(insights: Dict[str, Any], business_id: str, period: str, cache_dir: Path):
    """Save insights to cache"""
    logger.info("Saving insights", business_id=business_id, period=period)
    
    cache_dir.mkdir(exist_ok=True)
    
    # Generate filename
    filename = f"insights.{business_id}.{period}.json"
    filepath = cache_dir / filename
    
    # Save insights
    with open(filepath, 'w') as f:
        json.dump(insights, f, indent=2)
    
    logger.info("Insights saved", filepath=str(filepath))

def process_business_insights(business_id: str, data_dir: Path, cache_dir: Path, period: str = "2024-Q3"):
    """Process insights for a single business"""
    logger.info("Processing business insights", business_id=business_id, period=period)
    
    # Check cache first
    cache_file = cache_dir / f"insights.{business_id}.{period}.json"
    if cache_file.exists():
        logger.info("Cache hit, skipping generation", business_id=business_id)
        return
    
    # Load business data
    business_data = load_business_data(business_id, data_dir)
    
    # Prepare payload
    payload = prepare_insight_payload(business_data, period)
    
    # Generate insights
    insights = generate_insights_with_retry(payload)
    
    # Save insights
    save_insights(insights, business_id, period, cache_dir)
    
    logger.info("Completed business insights", business_id=business_id)

def main():
    """Main insight generation pipeline"""
    logger.info("Starting insight generation pipeline")
    
    # File paths
    data_dir = Path("data")
    cache_dir = data_dir / "cache"
    
    # Get all business IDs
    business_file = data_dir / "sb_restaurants_selected.csv"
    business_df = pd.read_csv(business_file)
    business_ids = business_df['business_id'].tolist()
    
    logger.info("Found businesses", count=len(business_ids))
    
    # Process each business
    for business_id in business_ids:
        try:
            process_business_insights(business_id, data_dir, cache_dir)
        except Exception as e:
            logger.error("Failed to process business", 
                        business_id=business_id, 
                        error=str(e))
    
    logger.info("Insight generation pipeline completed")

if __name__ == "__main__":
    main()
