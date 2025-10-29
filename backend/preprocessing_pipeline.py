#!/usr/bin/env python3
"""
Step 2: Preprocessing & Sentiment Pipeline
Clean review text, apply VADER sentiment, tag themes, compute monthly trends
"""

import pandas as pd
import numpy as np
from pathlib import Path
import re
import json
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from rapidfuzz import fuzz
import structlog

# Setup logging
logger = structlog.get_logger()

# Initialize VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Fixed theme categories (8 themes)
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

def clean_text(text: str) -> str:
    """Clean review text: lowercase, strip punctuation, normalize whitespace"""
    if pd.isna(text) or text == '':
        return ''
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove extra punctuation but keep basic sentence structure
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def tag_themes(text: str, themes: dict) -> dict:
    """Tag text with themes using hybrid exact + fuzzy matching"""
    if not text:
        return {theme: False for theme in themes.keys()}
    
    theme_matches = {theme: False for theme in themes.keys()}
    
    for theme_name, theme_data in themes.items():
        keywords = theme_data['keywords']
        
        # First try exact matching
        for keyword in keywords:
            if keyword in text:
                theme_matches[theme_name] = True
                break
        
        # If no exact match, try fuzzy matching
        if not theme_matches[theme_name]:
            words = text.split()
            for keyword in keywords:
                for word in words:
                    if fuzz.ratio(keyword, word) >= 85:
                        theme_matches[theme_name] = True
                        break
                if theme_matches[theme_name]:
                    break
    
    return theme_matches

def analyze_sentiment(text: str) -> dict:
    """Analyze sentiment using VADER"""
    if not text:
        return {'compound': 0.0, 'positive': 0.0, 'neutral': 0.0, 'negative': 0.0, 'label': 'neutral'}
    
    scores = analyzer.polarity_scores(text)
    
    # Derive label from compound score
    compound = scores['compound']
    if compound >= 0.05:
        label = 'positive'
    elif compound <= -0.05:
        label = 'negative'
    else:
        label = 'neutral'
    
    scores['label'] = label
    return scores

def process_reviews_file(file_path: str) -> pd.DataFrame:
    """Process a single reviews CSV file"""
    logger.info("Processing reviews file", file_path=file_path)
    
    # Load reviews
    df = pd.read_csv(file_path)
    logger.info("Loaded reviews", count=len(df))
    
    # Clean text
    df['cleaned_text'] = df['text'].apply(clean_text)
    
    # Analyze sentiment
    sentiment_results = df['cleaned_text'].apply(analyze_sentiment)
    df['sentiment_compound'] = [s['compound'] for s in sentiment_results]
    df['sentiment_label'] = [s['label'] for s in sentiment_results]
    
    # Tag themes
    theme_results = df['cleaned_text'].apply(lambda x: tag_themes(x, FIXED_THEMES))
    
    # Add theme columns
    for theme in FIXED_THEMES.keys():
        df[f'theme_{theme}'] = [tr[theme] for tr in theme_results]
    
    # Parse date
    df['date'] = pd.to_datetime(df['date'])
    df['year_month'] = df['date'].dt.to_period('M')
    
    logger.info("Processed reviews", count=len(df))
    return df

def compute_monthly_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Compute monthly sentiment trends per theme"""
    logger.info("Computing monthly trends")
    
    monthly_data = []
    
    for year_month in df['year_month'].unique():
        month_df = df[df['year_month'] == year_month]
        
        row = {
            'year_month': year_month,
            'total_reviews': len(month_df),
            'avg_stars': month_df['stars'].mean(),
            'avg_sentiment_compound': month_df['sentiment_compound'].mean()
        }
        
        # Add theme-specific sentiment
        for theme in FIXED_THEMES.keys():
            theme_reviews = month_df[month_df[f'theme_{theme}'] == True]
            if len(theme_reviews) > 0:
                row[f'{theme}_sentiment'] = theme_reviews['sentiment_compound'].mean()
                row[f'{theme}_count'] = len(theme_reviews)
            else:
                row[f'{theme}_sentiment'] = 0.0
                row[f'{theme}_count'] = 0
        
        monthly_data.append(row)
    
    trends_df = pd.DataFrame(monthly_data)
    trends_df = trends_df.sort_values('year_month')
    
    logger.info("Computed monthly trends", months=len(trends_df))
    return trends_df

def save_processed_data(df: pd.DataFrame, trends_df: pd.DataFrame, business_id: str, output_dir: str):
    """Save processed data"""
    logger.info("Saving processed data", business_id=business_id)
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Save processed reviews
    reviews_file = output_path / f"{business_id}_processed_reviews.csv"
    df.to_csv(reviews_file, index=False)
    
    # Save monthly trends
    trends_file = output_path / f"{business_id}_monthly_trends.csv"
    trends_df.to_csv(trends_file, index=False)
    
    logger.info("Saved processed data", 
               reviews_file=str(reviews_file),
               trends_file=str(trends_file))

def main():
    """Main preprocessing pipeline"""
    logger.info("Starting preprocessing pipeline")
    
    # File paths
    data_dir = Path("data")
    reviews_dir = data_dir / "reviews"
    output_dir = data_dir / "processed"
    
    # Get all review files
    review_files = list(reviews_dir.glob("*_reviews.csv"))
    logger.info("Found review files", count=len(review_files))
    
    for review_file in review_files:
        business_id = review_file.stem.replace('_reviews', '')
        logger.info("Processing business", business_id=business_id)
        
        # Process reviews
        processed_df = process_reviews_file(str(review_file))
        
        # Compute trends
        trends_df = compute_monthly_trends(processed_df)
        
        # Save results
        save_processed_data(processed_df, trends_df, business_id, str(output_dir))
    
    logger.info("Preprocessing pipeline completed")

if __name__ == "__main__":
    main()
