#!/usr/bin/env python3
"""
Extract reviews for selected Santa Barbara restaurants
"""

import json
import pandas as pd
from pathlib import Path
import structlog

# Setup logging
logger = structlog.get_logger()

def load_selected_businesses(csv_path: str) -> pd.DataFrame:
    """Load selected businesses from CSV"""
    logger.info("Loading selected businesses", csv_path=csv_path)
    df = pd.read_csv(csv_path)
    business_ids = df['business_id'].tolist()
    logger.info("Loaded business IDs", count=len(business_ids), ids=business_ids)
    return business_ids

def extract_reviews_for_businesses(review_file: str, business_ids: list) -> pd.DataFrame:
    """Extract reviews for specific business IDs"""
    logger.info("Extracting reviews for businesses", count=len(business_ids))
    
    reviews = []
    business_ids_set = set(business_ids)
    
    with open(review_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if line_num % 100000 == 0:
                logger.info("Processed review lines", count=line_num)
            
            try:
                review = json.loads(line.strip())
                if review['business_id'] in business_ids_set:
                    reviews.append(review)
                    logger.info("Found review", 
                              business_id=review['business_id'],
                              review_id=review['review_id'])
            except json.JSONDecodeError as e:
                logger.warning("JSON decode error", line=line_num, error=str(e))
                continue
    
    logger.info("Extracted reviews", total=len(reviews))
    return pd.DataFrame(reviews)

def save_reviews_by_business(reviews_df: pd.DataFrame, output_dir: str):
    """Save reviews grouped by business"""
    logger.info("Saving reviews by business", output_dir=output_dir)
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    for business_id in reviews_df['business_id'].unique():
        business_reviews = reviews_df[reviews_df['business_id'] == business_id]
        
        # Select relevant columns
        columns = ['business_id', 'review_id', 'user_id', 'stars', 'date', 'text']
        business_reviews_selected = business_reviews[columns].copy()
        
        # Save to CSV
        output_file = output_path / f"{business_id}_reviews.csv"
        business_reviews_selected.to_csv(output_file, index=False)
        
        logger.info("Saved reviews for business", 
                   business_id=business_id,
                   review_count=len(business_reviews_selected),
                   output_file=str(output_file))

def main():
    """Main review extraction process"""
    logger.info("Starting review extraction")
    
    # File paths
    data_dir = Path("data")
    businesses_csv = data_dir / "sb_restaurants_selected.csv"
    review_file = data_dir / "yelp_academic_dataset_review.json"
    output_dir = data_dir / "reviews"
    
    # Load selected businesses
    business_ids = load_selected_businesses(str(businesses_csv))
    
    # Extract reviews
    reviews_df = extract_reviews_for_businesses(str(review_file), business_ids)
    
    # Save reviews by business
    save_reviews_by_business(reviews_df, str(output_dir))
    
    logger.info("Review extraction completed")

if __name__ == "__main__":
    main()
