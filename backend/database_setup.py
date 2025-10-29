#!/usr/bin/env python3
"""
Database Schema and Data Loading
Create SQLite database with 6 tables and load all processed data
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, Text, ForeignKey, Date, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import structlog

# Setup logging
logger = structlog.get_logger()

# Database setup
Base = declarative_base()

# ===== SCHEMA DEFINITIONS =====

class Business(Base):
    """Basic business information"""
    __tablename__ = 'businesses'
    
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    city = Column(String, nullable=False)
    category = Column(String)
    review_count = Column(Integer)
    stars = Column(Float)
    
    __table_args__ = (
        Index('idx_business_city', 'city'),
    )

class Review(Base):
    """Raw cleaned reviews with sentiment"""
    __tablename__ = 'reviews'
    
    id = Column(String, primary_key=True)
    business_id = Column(String, ForeignKey('businesses.id'), nullable=False)
    date = Column(Date)
    stars = Column(Integer)
    sentiment_compound = Column(Float)
    sentiment_label = Column(String)
    text = Column(Text)
    business_name = Column(String)
    
    __table_args__ = (
        Index('idx_review_business', 'business_id'),
        Index('idx_review_date', 'date'),
    )

class Theme(Base):
    """Fixed 8 themes per business with latest scores"""
    __tablename__ = 'themes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(String, ForeignKey('businesses.id'), nullable=False)
    theme = Column(String, nullable=False)
    score = Column(Float)
    delta = Column(Float)
    
    __table_args__ = (
        Index('idx_theme_business', 'business_id'),
        Index('idx_theme_name', 'theme'),
    )

class Trend(Base):
    """Monthly sentiment trends per theme"""
    __tablename__ = 'trends'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(String, ForeignKey('businesses.id'), nullable=False)
    month = Column(String)
    theme = Column(String)
    avg_sentiment = Column(Float)
    review_count = Column(Integer)
    
    __table_args__ = (
        Index('idx_trend_business', 'business_id'),
        Index('idx_trend_month', 'month'),
        Index('idx_trend_theme', 'theme'),
    )

class Keyword(Base):
    """Dynamic keywords extracted from reviews"""
    __tablename__ = 'keywords'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(String, ForeignKey('businesses.id'), nullable=False)
    term = Column(String, nullable=False)
    count = Column(Integer)
    tfidf = Column(Float)
    
    __table_args__ = (
        Index('idx_keyword_business', 'business_id'),
        Index('idx_keyword_term', 'term'),
    )

class Insight(Base):
    """Final generated JSON insights"""
    __tablename__ = 'insights'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(String, ForeignKey('businesses.id'), nullable=False)
    period = Column(String, nullable=False)
    json_output = Column(Text)
    generated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_insight_business', 'business_id'),
    )

# ===== DATA LOADING FUNCTIONS =====

def load_businesses(data_dir: Path, session):
    """Load business metadata"""
    logger.info("Loading businesses")
    
    business_file = data_dir / "sb_restaurants_selected.csv"
    businesses_df = pd.read_csv(business_file)
    
    for _, row in businesses_df.iterrows():
        business = Business(
            id=row['business_id'],
            name=row['name'],
            city=row['city'],
            category=row['categories'],
            review_count=int(row['review_count']),
            stars=float(row['stars'])
        )
        session.add(business)
    
    session.commit()
    logger.info("Loaded businesses", count=len(businesses_df))

def load_reviews(data_dir: Path, session):
    """Load processed reviews"""
    logger.info("Loading reviews")
    
    processed_dir = data_dir / "processed"
    review_files = list(processed_dir.glob("*_processed_reviews.csv"))
    
    total_reviews = 0
    for review_file in review_files:
        business_id = review_file.stem.replace('_processed_reviews', '')
        reviews_df = pd.read_csv(review_file)
        
        for _, row in reviews_df.iterrows():
            review = Review(
                id=row['review_id'],
                business_id=business_id,
                date=pd.to_datetime(row['date']).date(),
                stars=int(row['stars']),
                sentiment_compound=float(row['sentiment_compound']),
                sentiment_label=row['sentiment_label'],
                text=row['text'],
                business_name=row.get('business_name', '')
            )
            session.add(review)
        
        total_reviews += len(reviews_df)
        
        if len(reviews_df) % 100 == 0:  # Commit in batches
            session.commit()
    
    session.commit()
    logger.info("Loaded reviews", count=total_reviews)

def load_themes(data_dir: Path, session):
    """Load theme scores and deltas"""
    logger.info("Loading themes")
    
    processed_dir = data_dir / "processed"
    trends_files = list(processed_dir.glob("*_monthly_trends.csv"))
    
    theme_names = ['food_quality', 'service', 'speed_wait', 'ambiance', 'cleanliness', 
                   'portion_size', 'price_value', 'staff_behavior']
    
    for trends_file in trends_files:
        business_id = trends_file.stem.replace('_monthly_trends', '')
        trends_df = pd.read_csv(trends_file)
        
        # Get latest scores and deltas for each theme
        for theme_name in theme_names:
            if len(trends_df) > 0:
                latest_score = float(trends_df[f'{theme_name}_sentiment'].iloc[-1])
                
                # Calculate delta
                delta = 0.0
                if len(trends_df) > 1:
                    prev_score = float(trends_df[f'{theme_name}_sentiment'].iloc[-2])
                    delta = latest_score - prev_score
                
                # Only add if there's data
                if latest_score != 0.0 or abs(delta) > 0.01:
                    theme = Theme(
                        business_id=business_id,
                        theme=theme_name,
                        score=latest_score,
                        delta=delta
                    )
                    session.add(theme)
        
        session.commit()
    
    logger.info("Loaded themes")

def load_trends(data_dir: Path, session):
    """Load monthly trend data"""
    logger.info("Loading trends")
    
    processed_dir = data_dir / "processed"
    trends_files = list(processed_dir.glob("*_monthly_trends.csv"))
    
    theme_names = ['food_quality', 'service', 'speed_wait', 'ambiance', 'cleanliness',
                   'portion_size', 'price_value', 'staff_behavior']
    
    total_rows = 0
    for trends_file in trends_files:
        business_id = trends_file.stem.replace('_monthly_trends', '')
        trends_df = pd.read_csv(trends_file)
        
        for _, row in trends_df.iterrows():
            month = str(row['year_month'])
            
            for theme_name in theme_names:
                sentiment = float(row[f'{theme_name}_sentiment'])
                count = int(row[f'{theme_name}_count'])
                
                if count > 0:  # Only add if there are reviews for this theme
                    trend = Trend(
                        business_id=business_id,
                        month=month,
                        theme=theme_name,
                        avg_sentiment=sentiment,
                        review_count=count
                    )
                    session.add(trend)
                    total_rows += 1
        
        if total_rows % 1000 == 0:  # Commit in batches
            session.commit()
    
    session.commit()
    logger.info("Loaded trends", count=total_rows)

def load_keywords(data_dir: Path, session):
    """Load dynamic keywords"""
    logger.info("Loading keywords")
    
    keywords_dir = data_dir / "keywords_quotes"
    keyword_files = list(keywords_dir.glob("*_keywords.json"))
    
    total_keywords = 0
    for keyword_file in keyword_files:
        business_id = keyword_file.stem.replace('_keywords', '')
        
        with open(keyword_file, 'r') as f:
            keywords_data = json.load(f)
        
        for kw in keywords_data:
            keyword = Keyword(
                business_id=business_id,
                term=kw['term'],
                count=int(kw['count']),
                tfidf=float(kw['tfidf'])
            )
            session.add(keyword)
            total_keywords += 1
        
        if total_keywords % 100 == 0:  # Commit in batches
            session.commit()
    
    session.commit()
    logger.info("Loaded keywords", count=total_keywords)

def load_insights(data_dir: Path, session):
    """Load generated insights"""
    logger.info("Loading insights")
    
    cache_dir = data_dir / "cache"
    insight_files = list(cache_dir.glob("insights.*.json"))
    
    for insight_file in insight_files:
        # Parse filename: insights.{business_id}.{period}.json
        parts = insight_file.stem.split('.')
        if len(parts) >= 3:
            business_id = parts[1]
            period = parts[2]
            
            with open(insight_file, 'r') as f:
                insights_json = json.dumps(json.load(f))
            
            insight = Insight(
                business_id=business_id,
                period=period,
                json_output=insights_json,
                generated_at=datetime.fromtimestamp(insight_file.stat().st_mtime)
            )
            session.add(insight)
    
    session.commit()
    logger.info("Loaded insights", count=len(insight_files))

def verify_database(session):
    """Verify database contents"""
    logger.info("Verifying database")
    
    business_count = session.query(Business).count()
    review_count = session.query(Review).count()
    theme_count = session.query(Theme).count()
    trend_count = session.query(Trend).count()
    keyword_count = session.query(Keyword).count()
    insight_count = session.query(Insight).count()
    
    logger.info("Database verification",
                businesses=business_count,
                reviews=review_count,
                themes=theme_count,
                trends=trend_count,
                keywords=keyword_count,
                insights=insight_count)
    
    # Check business count
    if business_count < 6:
        logger.error("Expected 6 businesses, found", count=business_count)
        return False
    
    if insight_count < 6:
        logger.error("Expected 6 insights, found", count=insight_count)
        return False
    
    logger.info("Database verification passed")
    return True

def main():
    """Main database setup and loading pipeline"""
    logger.info("Starting database setup")
    
    # Database file
    db_file = Path("bizvista.db")
    
    # Remove existing database
    if db_file.exists():
        logger.info("Removing existing database")
        db_file.unlink()
    
    # Create engine and tables
    engine = create_engine(f'sqlite:///{db_file}')
    Base.metadata.create_all(engine)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Load all data
        data_dir = Path("data")
        
        load_businesses(data_dir, session)
        load_reviews(data_dir, session)
        load_themes(data_dir, session)
        load_trends(data_dir, session)
        load_keywords(data_dir, session)
        load_insights(data_dir, session)
        
        # Verify
        if verify_database(session):
            logger.info("Database setup completed successfully")
            
            # Check file size
            db_size_mb = db_file.stat().st_size / (1024 * 1024)
            logger.info("Database file size", size_mb=f"{db_size_mb:.2f}")
            
            if db_size_mb > 50:
                logger.warning("Database size exceeds 50 MB", size_mb=f"{db_size_mb:.2f}")
        else:
            logger.error("Database verification failed")
    
    finally:
        session.close()
    
    logger.info("Database setup pipeline completed")

if __name__ == "__main__":
    main()
