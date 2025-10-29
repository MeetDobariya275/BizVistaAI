#!/usr/bin/env python3
"""
Step 3: Dynamic Keywords & Representative Quotes
Extract keywords using TF-IDF and generate representative quotes per theme
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import re
from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer
from rapidfuzz import fuzz
import structlog

# Setup logging
logger = structlog.get_logger()

# Import FIXED_THEMES from preprocessing pipeline
FIXED_THEMES = {
    'food_quality': {
        'keywords': ['taste', 'flavor', 'delicious', 'tasty', 'bland', 'spicy', 'fresh', 'cooked', 'raw', 'burnt', 'seasoning', 'sauce', 'seasoned', 'flavorful', 'bland', 'bitter', 'sweet', 'salty', 'sour']
    },
    'service': {
        'keywords': ['service', 'server', 'waiter', 'waitress', 'staff', 'friendly', 'attentive', 'helpful', 'polite', 'rude', 'ignored', 'welcoming', 'courteous', 'professional', 'unfriendly']
    },
    'speed_wait': {
        'keywords': ['wait', 'time', 'slow', 'fast', 'quick', 'delay', 'rushed', 'hurried', 'patience', 'timely', 'prompt', 'late', 'early', 'minutes', 'hours', 'seating', 'table']
    },
    'ambiance': {
        'keywords': ['atmosphere', 'ambiance', 'noise', 'loud', 'quiet', 'decor', 'vibe', 'romantic', 'casual', 'elegant', 'cozy', 'crowded', 'empty', 'music', 'lighting', 'mood']
    },
    'cleanliness': {
        'keywords': ['clean', 'dirty', 'hygiene', 'tidy', 'messy', 'sanitary', 'bathroom', 'restroom', 'table', 'floor', 'kitchen', 'spotless', 'filthy', 'neat', 'organized']
    },
    'portion_size': {
        'keywords': ['portion', 'size', 'small', 'large', 'huge', 'tiny', 'generous', 'skimpy', 'enough', 'plenty', 'scanty', 'massive', 'mini', 'big', 'little']
    },
    'price_value': {
        'keywords': ['price', 'expensive', 'cheap', 'value', 'worth', 'overpriced', 'affordable', 'budget', 'cost', 'money', 'dollar', 'pay', 'bill', 'reasonable', 'rip-off']
    },
    'staff_behavior': {
        'keywords': ['professional', 'attitude', 'behavior', 'rude', 'polite', 'arrogant', 'humble', 'patient', 'impatient', 'knowledgeable', 'ignorant', 'competent', 'incompetent']
    }
}

# Configuration
MIN_MENTIONS_CONFIG = {
    'high': 10,    # reviews >= 1000
    'medium': 7,   # reviews 500-999
    'low': 5       # reviews < 500
}

def get_min_mentions_threshold(review_count: int) -> int:
    """Get minimum mentions threshold based on review count"""
    if review_count >= 1000:
        return MIN_MENTIONS_CONFIG['high']
    elif review_count >= 500:
        return MIN_MENTIONS_CONFIG['medium']
    else:
        return MIN_MENTIONS_CONFIG['low']

def clean_text_for_keywords(text: str) -> str:
    """Clean text for keyword extraction"""
    if pd.isna(text) or text == '':
        return ''
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove punctuation and numbers
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\d+', ' ', text)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def simple_stem_normalize(text: str) -> str:
    """Simple stemming/normalization for common endings"""
    # Remove common endings
    text = re.sub(r'(s|es)$', '', text)
    text = re.sub(r'(ing|ed)$', '', text)
    text = re.sub(r'(ly)$', '', text)
    
    return text.strip()

def extract_keywords_tfidf(texts: list, min_mentions: int) -> list:
    """Extract keywords using TF-IDF"""
    logger.info("Extracting keywords with TF-IDF", 
               text_count=len(texts), 
               min_mentions=min_mentions)
    
    # Clean texts
    cleaned_texts = [clean_text_for_keywords(text) for text in texts]
    cleaned_texts = [text for text in cleaned_texts if text.strip()]
    
    if not cleaned_texts:
        return []
    
    # TF-IDF vectorizer
    vectorizer = TfidfVectorizer(
        ngram_range=(1, 3),
        min_df=5,
        max_df=0.5,
        max_features=3000,
        stop_words='english'
    )
    
    try:
        tfidf_matrix = vectorizer.fit_transform(cleaned_texts)
        feature_names = vectorizer.get_feature_names_out()
        
        # Get TF-IDF scores
        tfidf_scores = tfidf_matrix.sum(axis=0).A1
        
        # Create keyword list with counts and scores
        keywords = []
        for i, (term, score) in enumerate(zip(feature_names, tfidf_scores)):
            # Count actual mentions
            count = sum(1 for text in cleaned_texts if term in text)
            
            if count >= min_mentions:
                keywords.append({
                    'term': term,
                    'count': count,
                    'tfidf': float(score)
                })
        
        # Sort by TF-IDF score
        keywords.sort(key=lambda x: x['tfidf'], reverse=True)
        
        logger.info("Extracted keywords", count=len(keywords))
        return keywords
        
    except Exception as e:
        logger.error("TF-IDF extraction failed", error=str(e))
        return []

def merge_similar_keywords(keywords: list) -> list:
    """Merge near-duplicate keywords using fuzzy matching"""
    if not keywords:
        return []
    
    merged = []
    used_indices = set()
    
    for i, kw1 in enumerate(keywords):
        if i in used_indices:
            continue
            
        # Find similar keywords
        similar_group = [kw1]
        for j, kw2 in enumerate(keywords[i+1:], i+1):
            if j in used_indices:
                continue
                
            if fuzz.ratio(kw1['term'], kw2['term']) >= 90:
                similar_group.append(kw2)
                used_indices.add(j)
        
        # Keep the one with highest TF-IDF score
        best_kw = max(similar_group, key=lambda x: x['tfidf'])
        merged.append(best_kw)
        used_indices.add(i)
    
    logger.info("Merged similar keywords", 
               original=len(keywords), 
               merged=len(merged))
    
    return merged

def extract_representative_quotes(df: pd.DataFrame, themes: dict, keywords: list) -> dict:
    """Extract representative quotes per theme"""
    logger.info("Extracting representative quotes")
    
    quotes_by_theme = {}
    
    # Create keyword lookup for theme relevance
    keyword_terms = [kw['term'] for kw in keywords]
    
    for theme_name in themes.keys():
        theme_quotes = {'positive': [], 'negative': []}
        
        # Get reviews tagged with this theme
        theme_reviews = df[df[f'theme_{theme_name}'] == True].copy()
        
        if len(theme_reviews) == 0:
            quotes_by_theme[theme_name] = theme_quotes
            continue
        
        # Separate positive and negative reviews
        positive_reviews = theme_reviews[theme_reviews['sentiment_compound'] >= 0.4]
        negative_reviews = theme_reviews[theme_reviews['sentiment_compound'] <= -0.2]
        
        # Extract quotes with preference for keyword-containing reviews
        def extract_quotes_from_reviews(reviews, sentiment_type, max_quotes=2):
            quotes = []
            used_reviews = set()
            
            # First pass: prefer reviews with theme keywords
            for _, review in reviews.iterrows():
                if len(quotes) >= max_quotes:
                    break
                    
                if review['review_id'] in used_reviews:
                    continue
                
                text = str(review['text'])
                if len(text) <= 160:
                    # Check if review contains relevant keywords
                    text_lower = text.lower()
                    has_keyword = any(kw in text_lower for kw in keyword_terms)
                    
                    if has_keyword or len(quotes) < 1:  # Always take at least one
                        quotes.append(text)
                        used_reviews.add(review['review_id'])
            
            # Second pass: fill remaining slots
            for _, review in reviews.iterrows():
                if len(quotes) >= max_quotes:
                    break
                    
                if review['review_id'] in used_reviews:
                    continue
                
                text = str(review['text'])
                if len(text) <= 160:
                    quotes.append(text)
                    used_reviews.add(review['review_id'])
                elif len(text) > 160:
                    # Truncate carefully
                    truncated = text[:157] + "..."
                    quotes.append(truncated)
                    used_reviews.add(review['review_id'])
            
            return quotes[:max_quotes]
        
        # Extract quotes
        theme_quotes['positive'] = extract_quotes_from_reviews(positive_reviews, 'positive', 2)
        theme_quotes['negative'] = extract_quotes_from_reviews(negative_reviews, 'negative', 2)
        
        quotes_by_theme[theme_name] = theme_quotes
    
    logger.info("Extracted quotes by theme", 
               themes=len(quotes_by_theme))
    
    return quotes_by_theme

def deduplicate_quotes(quotes_by_theme: dict) -> dict:
    """Remove duplicate/similar quotes"""
    logger.info("Deduplicating quotes")
    
    for theme_name, quotes in quotes_by_theme.items():
        for sentiment in ['positive', 'negative']:
            if not quotes[sentiment]:
                continue
                
            unique_quotes = []
            for quote in quotes[sentiment]:
                is_duplicate = False
                for existing in unique_quotes:
                    if fuzz.ratio(quote, existing) >= 90:
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    unique_quotes.append(quote)
            
            quotes_by_theme[theme_name][sentiment] = unique_quotes
    
    return quotes_by_theme

def process_business_keywords_quotes(business_id: str, processed_file: str) -> tuple:
    """Process keywords and quotes for a single business"""
    logger.info("Processing keywords and quotes", business_id=business_id)
    
    # Load processed reviews
    df = pd.read_csv(processed_file)
    logger.info("Loaded processed reviews", count=len(df))
    
    # Get review texts
    texts = df['text'].fillna('').tolist()
    
    # Determine minimum mentions threshold
    min_mentions = get_min_mentions_threshold(len(df))
    
    # Extract keywords
    keywords = extract_keywords_tfidf(texts, min_mentions)
    keywords = merge_similar_keywords(keywords)
    
    # Extract quotes
    quotes_by_theme = extract_representative_quotes(df, FIXED_THEMES, keywords)
    quotes_by_theme = deduplicate_quotes(quotes_by_theme)
    
    logger.info("Processed business", 
               business_id=business_id,
               keywords=len(keywords),
               themes=len(quotes_by_theme))
    
    return keywords, quotes_by_theme

def save_keywords_quotes(keywords: list, quotes_by_theme: dict, business_id: str, output_dir: str):
    """Save keywords and quotes to JSON files"""
    logger.info("Saving keywords and quotes", business_id=business_id)
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Save keywords
    keywords_file = output_path / f"{business_id}_keywords.json"
    with open(keywords_file, 'w') as f:
        json.dump(keywords, f, indent=2)
    
    # Save quotes
    quotes_file = output_path / f"{business_id}_quotes.json"
    with open(quotes_file, 'w') as f:
        json.dump(quotes_by_theme, f, indent=2)
    
    logger.info("Saved files", 
               keywords_file=str(keywords_file),
               quotes_file=str(quotes_file))

def quality_check(keywords: list, quotes_by_theme: dict, business_id: str) -> bool:
    """Perform quality checks"""
    logger.info("Performing quality checks", business_id=business_id)
    
    issues = []
    
    # Check top 20 keywords
    top_keywords = keywords[:20]
    for kw in top_keywords:
        if len(kw['term']) < 2 or kw['term'].isdigit():
            issues.append(f"Junk keyword: {kw['term']}")
    
    # Check quotes per theme
    for theme_name, quotes in quotes_by_theme.items():
        pos_count = len(quotes['positive'])
        neg_count = len(quotes['negative'])
        
        if pos_count < 1:
            issues.append(f"{theme_name}: No positive quotes")
        if neg_count < 1:
            issues.append(f"{theme_name}: No negative quotes")
        
        # Check quote length
        for sentiment in ['positive', 'negative']:
            for quote in quotes[sentiment]:
                if len(quote) > 160:
                    issues.append(f"{theme_name} {sentiment}: Quote too long ({len(quote)} chars)")
    
    if issues:
        logger.warning("Quality issues found", issues=issues)
        return False
    else:
        logger.info("Quality checks passed")
        return True

def main():
    """Main keywords and quotes extraction pipeline"""
    logger.info("Starting keywords and quotes extraction")
    
    # File paths
    data_dir = Path("data")
    processed_dir = data_dir / "processed"
    output_dir = data_dir / "keywords_quotes"
    
    # Get all processed review files
    processed_files = list(processed_dir.glob("*_processed_reviews.csv"))
    logger.info("Found processed files", count=len(processed_files))
    
    for processed_file in processed_files:
        business_id = processed_file.stem.replace('_processed_reviews', '')
        logger.info("Processing business", business_id=business_id)
        
        # Process keywords and quotes
        keywords, quotes_by_theme = process_business_keywords_quotes(
            business_id, str(processed_file)
        )
        
        # Quality check
        quality_ok = quality_check(keywords, quotes_by_theme, business_id)
        
        # Save results
        save_keywords_quotes(keywords, quotes_by_theme, business_id, str(output_dir))
        
        logger.info("Completed business", 
                   business_id=business_id,
                   quality_ok=quality_ok)
    
    logger.info("Keywords and quotes extraction completed")

if __name__ == "__main__":
    main()
