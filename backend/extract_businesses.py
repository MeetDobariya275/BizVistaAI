#!/usr/bin/env python3
"""
Step 1: Dataset Extraction & Business Selection
Extract Santa Barbara restaurants with ≥1000 reviews from Yelp dataset
"""

import json
import pandas as pd
from pathlib import Path
import structlog

# Setup logging
logger = structlog.get_logger()

def load_businesses(file_path: str) -> pd.DataFrame:
    """Load business data from JSON file"""
    logger.info("Loading business data", file_path=file_path)
    
    businesses = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % 10000 == 0:
                logger.info("Processed lines", count=line_num)
            
            try:
                business = json.loads(line.strip())
                businesses.append(business)
            except json.JSONDecodeError as e:
                logger.warning("JSON decode error", line=line_num, error=str(e))
                continue
    
    logger.info("Loaded businesses", total=len(businesses))
    return pd.DataFrame(businesses)

def filter_sb_restaurants(df: pd.DataFrame) -> pd.DataFrame:
    """Filter for Santa Barbara restaurants with ≥1000 reviews"""
    logger.info("Filtering Santa Barbara restaurants")
    
    # Filter for Santa Barbara
    sb_businesses = df[df['city'].str.contains('Santa Barbara', case=False, na=False)]
    logger.info("SB businesses", count=len(sb_businesses))
    
    # Filter for restaurants (categories containing 'Restaurants')
    restaurants = sb_businesses[
        sb_businesses['categories'].str.contains('Restaurants', case=False, na=False)
    ]
    logger.info("SB restaurants", count=len(restaurants))
    
    # Show review count distribution
    logger.info("Review count stats", 
               min=restaurants['review_count'].min(),
               max=restaurants['review_count'].max(),
               mean=restaurants['review_count'].mean())
    
    # Filter for businesses with ≥500 reviews (lowering threshold since SB is smaller)
    high_review_restaurants = restaurants[restaurants['review_count'] >= 500]
    logger.info("High review restaurants", count=len(high_review_restaurants))
    
    return high_review_restaurants

def select_top_restaurants(df: pd.DataFrame, n: int = 6) -> pd.DataFrame:
    """Select top N restaurants by review count"""
    logger.info("Selecting top restaurants", n=n)
    
    # Sort by review count descending and select top N
    top_restaurants = df.nlargest(n, 'review_count')
    
    logger.info("Selected restaurants", count=len(top_restaurants))
    for idx, row in top_restaurants.iterrows():
        logger.info("Restaurant", 
                   name=row['name'], 
                   review_count=row['review_count'],
                   business_id=row['business_id'])
    
    return top_restaurants

def save_business_selection(df: pd.DataFrame, output_path: str):
    """Save selected businesses to CSV"""
    logger.info("Saving business selection", output_path=output_path)
    
    # Select relevant columns
    columns = ['business_id', 'name', 'city', 'categories', 'review_count', 'stars']
    df_selected = df[columns].copy()
    
    df_selected.to_csv(output_path, index=False)
    logger.info("Saved businesses", count=len(df_selected))

def main():
    """Main extraction process"""
    logger.info("Starting business extraction")
    
    # File paths
    data_dir = Path("data")
    business_file = data_dir / "yelp_academic_dataset_business.json"
    output_file = data_dir / "sb_restaurants_selected.csv"
    
    # Load and process data
    businesses_df = load_businesses(str(business_file))
    sb_restaurants = filter_sb_restaurants(businesses_df)
    top_restaurants = select_top_restaurants(sb_restaurants, n=6)
    
    # Save results
    save_business_selection(top_restaurants, str(output_file))
    
    logger.info("Business extraction completed")

if __name__ == "__main__":
    main()