#!/usr/bin/env python3
"""
Database Index Creation
Add indexes to optimize refresh queries and ensure data integrity
"""

from sqlalchemy import create_engine, Index, text
from pathlib import Path
from backend.api import Base

# Database setup
project_root = Path(__file__).parent.parent
db_path = project_root / 'bizvista.db'
engine = create_engine(f'sqlite:///{db_path}')

def create_indexes():
    """Create indexes for performance and constraints"""
    print(f"Creating indexes on {db_path}")
    
    with engine.connect() as conn:
        # Index for reviews by business_id and date (for period filtering)
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_reviews_biz_date 
            ON reviews(business_id, date)
        """))
        
        # Unique constraint on themes (business_id, theme)
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_themes_biz_theme 
            ON themes(business_id, theme)
        """))
        
        # Unique constraint on trends (business_id, month, theme)
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_trends_biz_month_theme 
            ON trends(business_id, month, theme)
        """))
        
        # Index for trends by business and month
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_trends_biz_month 
            ON trends(business_id, month)
        """))
        
        # Index for keywords by business
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_keywords_biz 
            ON keywords(business_id)
        """))
        
        conn.commit()
        print("✅ Indexes created successfully")

if __name__ == "__main__":
    create_indexes()

