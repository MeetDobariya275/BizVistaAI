#!/usr/bin/env python3
"""
FastAPI Backend - v1 (read-only)
Endpoints for querying business data from SQLite database
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Float, Text, ForeignKey, Date, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
import json
import hashlib
import requests
from datetime import datetime, timedelta
from pathlib import Path
import structlog
from rapidfuzz import fuzz
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pydantic import BaseModel
from typing import List as TypingList

# Setup logging
logger = structlog.get_logger()

# Initialize FastAPI app
app = FastAPI(title="BizVista AI API", version="1.0")

# CORS middleware for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4173", "http://127.0.0.1:4173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database setup
# Get the project root directory (parent of backend/)
project_root = Path(__file__).parent.parent
db_path = project_root / 'bizvista.db'
engine = create_engine(f'sqlite:///{db_path}')
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Ollama configuration
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "phi3:mini"

# Import database models (same as database_setup.py)
class Business(Base):
    __tablename__ = 'businesses'
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    city = Column(String, nullable=False)
    category = Column(String)
    review_count = Column(Integer)
    stars = Column(Float)

class Review(Base):
    __tablename__ = 'reviews'
    id = Column(String, primary_key=True)
    business_id = Column(String, ForeignKey('businesses.id'), nullable=False)
    date = Column(Date)
    stars = Column(Integer)
    sentiment_compound = Column(Float)
    sentiment_label = Column(String)
    text = Column(Text)

class Theme(Base):
    __tablename__ = 'themes'
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(String, ForeignKey('businesses.id'), nullable=False)
    theme = Column(String, nullable=False)
    score = Column(Float)
    delta = Column(Float)

class Trend(Base):
    __tablename__ = 'trends'
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(String, ForeignKey('businesses.id'), nullable=False)
    month = Column(String)
    theme = Column(String)
    avg_sentiment = Column(Float)
    review_count = Column(Integer)

class Keyword(Base):
    __tablename__ = 'keywords'
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(String, ForeignKey('businesses.id'), nullable=False)
    term = Column(String, nullable=False)
    count = Column(Integer)
    tfidf = Column(Float)

class Insight(Base):
    __tablename__ = 'insights'
    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(String, ForeignKey('businesses.id'), nullable=False)
    period = Column(String, nullable=False)
    json_output = Column(Text)
    generated_at = Column(DateTime)

# Dependency for database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Cache functions
def get_cache_key(business_ids, theme_winners_data):
    """Generate cache key for comparison"""
    sorted_ids = ','.join(sorted(business_ids))
    theme_hash = hashlib.sha256(json.dumps(theme_winners_data, sort_keys=True).encode()).hexdigest()[:16]
    key = f"cmp-narrative|{sorted_ids}|latest|{theme_hash}"
    return hashlib.sha256(key.encode()).hexdigest()

def load_cache(cache_key):
    """Load cached comparison result"""
    cache_dir = project_root / "data" / "cache"
    cache_file = cache_dir / f"comparison.{cache_key}.json"
    
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            return json.load(f)
    return None

def save_cache(cache_key, data):
    """Save comparison result to cache"""
    cache_dir = project_root / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    cache_file = cache_dir / f"comparison.{cache_key}.json"
    with open(cache_file, 'w') as f:
        json.dump(data, f, indent=2)

def call_ollama(prompt_text, temperature=0.3, seed=42, max_retries=2):
    """Call Ollama API to generate narrative"""
    params = {
        "model": OLLAMA_MODEL,
        "prompt": prompt_text,
        "temperature": temperature,
        "num_ctx": 1024,
        "seed": seed,
        "stream": False
    }
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(OLLAMA_URL, json=params, timeout=10)
            response.raise_for_status()
            return response.json().get('response', '')
        except Exception as e:
            if attempt == max_retries:
                raise
            logger.warning("Ollama call failed, retrying", attempt=attempt+1, error=str(e))
    
    return ""

def validate_narrative_output(text):
    """Validate LLM narrative output"""
    try:
        data = json.loads(text)
        
        # Check required keys
        required = ['summary', 'by_theme', 'risks', 'opportunities']
        if not all(k in data for k in required):
            return None, "Missing required keys"
        
        # Check types and lengths
        if not isinstance(data['summary'], str):
            return None, "Summary must be string"
        if not isinstance(data['by_theme'], list) or len(data['by_theme']) > 5:
            return None, "by_theme must be array with max 5 items"
        if not isinstance(data['risks'], list) or len(data['risks']) != 2:
            return None, "risks must be array with exactly 2 items"
        if not isinstance(data['opportunities'], list) or len(data['opportunities']) != 3:
            return None, "opportunities must be array with exactly 3 items"
        
        # Word count check
        total_words = sum(len(str(v).split()) for k, v in data.items() if k in required)
        if total_words > 160:
            return None, f"Total words {total_words} exceeds 160 limit"
        
        return data, None
    except json.JSONDecodeError as e:
        return None, f"JSON parse error: {str(e)}"

# ===== API ENDPOINTS =====

@app.get("/api/businesses")
async def get_businesses():
    """Get list of all businesses"""
    db = next(get_db())
    
    try:
        businesses = db.query(Business).all()
        
        result = [
            {
                "id": b.id,
                "name": b.name,
                "city": b.city,
                "category": b.category,
                "review_count": b.review_count,
                "stars": b.stars
            }
            for b in businesses
        ]
        
        logger.info("Returned businesses", count=len(result))
        return result
    
    except Exception as e:
        logger.error("Error fetching businesses", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/api/businesses/{business_id}/overview")
async def get_business_overview(business_id: str):
    """Get business overview with themes, keywords, and insights"""
    db = next(get_db())
    
    try:
        # Get business
        business = db.query(Business).filter(Business.id == business_id).first()
        if not business:
            raise HTTPException(status_code=404, detail="Business not found")
        
        # Get themes
        themes = db.query(Theme).filter(Theme.business_id == business_id).all()
        themes_data = [
            {
                "theme": t.theme,
                "score": t.score,
                "delta": t.delta
            }
            for t in themes
        ]
        
        # Get keywords (top 10 by TF-IDF)
        keywords = db.query(Keyword).filter(Keyword.business_id == business_id) \
                    .order_by(Keyword.tfidf.desc()).limit(10).all()
        keywords_data = [
            {
                "term": k.term,
                "count": k.count,
                "tfidf": k.tfidf
            }
            for k in keywords
        ]
        
        # Get insights
        insight = db.query(Insight).filter(Insight.business_id == business_id) \
                  .order_by(Insight.generated_at.desc()).first()
        
        insights_data = None
        last_run = None
        if insight:
            insights_data = json.loads(insight.json_output)
            last_run = insight.generated_at.isoformat() if insight.generated_at else None
        
        result = {
            "business": {
                "id": business.id,
                "name": business.name,
                "city": business.city,
                "stars": business.stars
            },
            "themes": themes_data,
            "keywords": keywords_data,
            "insights": insights_data if insights_data else {},
            "last_run": last_run
        }
        
        logger.info("Returned business overview", business_id=business_id)
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching business overview", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/api/businesses/{business_id}/trends")
async def get_business_trends(business_id: str):
    """Get monthly trend data for a business"""
    db = next(get_db())
    
    try:
        # Get all trends for this business
        trends = db.query(Trend).filter(Trend.business_id == business_id).all()
        
        if not trends:
            return []
        
        # Calculate overall sentiment by month
        monthly_data = {}
        for trend in trends:
            if trend.month not in monthly_data:
                monthly_data[trend.month] = {"month": trend.month, "avg_sentiment": 0, "review_count": 0}
            
            monthly_data[trend.month]["review_count"] += trend.review_count
            monthly_data[trend.month]["avg_sentiment"] = (
                monthly_data[trend.month]["avg_sentiment"] + trend.avg_sentiment * trend.review_count
            ) / monthly_data[trend.month]["review_count"] if monthly_data[trend.month]["review_count"] > 0 else 0
        
        # Convert to list and sort by month
        result = sorted(monthly_data.values(), key=lambda x: x["month"])
        
        logger.info("Returned trends", business_id=business_id, count=len(result))
        return result
    
    except Exception as e:
        logger.error("Error fetching trends", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/api/compare-narrative")
async def compare_businesses_narrative(ids: str):
    """Compare businesses with narrative insights (max 3)"""
    db = next(get_db())
    
    try:
        # Parse business IDs
        business_ids = [bid.strip() for bid in ids.split(',')]
        
        if len(business_ids) > 3:
            raise HTTPException(status_code=400, detail="Maximum 3 businesses allowed")
        
        if len(business_ids) < 2:
            raise HTTPException(status_code=400, detail="At least 2 businesses required")
        
        # Get business info
        businesses = db.query(Business).filter(Business.id.in_(business_ids)).all()
        business_map = {b.id: b.name for b in businesses}
        
        if len(businesses) != len(business_ids):
            raise HTTPException(status_code=404, detail="One or more businesses not found")
        
        # Get all themes for these businesses
        themes = db.query(Theme).filter(Theme.business_id.in_(business_ids)).all()
        
        # Build per-business theme data
        business_themes = {}
        for business_id in business_ids:
            business_themes[business_id] = {}
            for theme in themes:
                if theme.business_id == business_id:
                    business_themes[business_id][theme.theme] = {
                        'score': theme.score,
                        'delta': theme.delta
                    }
        
        # Compute per-theme leaders
        theme_leaders = {}
        all_theme_names = set()
        for bid in business_ids:
            for theme_name in business_themes[bid]:
                all_theme_names.add(theme_name)
                if theme_name not in theme_leaders:
                    theme_leaders[theme_name] = []
                theme_leaders[theme_name].append({
                    'business_id': bid,
                    'score': business_themes[bid][theme_name]['score']
                })
        
        # Find winner per theme
        theme_winners = {}
        for theme_name in theme_leaders:
            sorted_themes = sorted(theme_leaders[theme_name], key=lambda x: x['score'], reverse=True)
            winner = sorted_themes[0]
            margin = sorted_themes[0]['score'] - sorted_themes[1]['score'] if len(sorted_themes) > 1 else 0
            theme_winners[theme_name] = {
                'leader': winner['business_id'],
                'leader_name': business_map[winner['business_id']],
                'score': winner['score'],
                'margin': margin
            }
        
        # Compute overall leader
        business_totals = {}
        for bid in business_ids:
            total = sum(business_themes[bid][t]['score'] for t in business_themes[bid])
            count = len(business_themes[bid])
            business_totals[bid] = total / count if count > 0 else 0
        
        overall_leader_id = max(business_totals.keys(), key=lambda k: business_totals[k])
        overall_leader_name = business_map[overall_leader_id]
        
        # Build summary text (deterministic for now)
        summary = f"{overall_leader_name} leads overall in customer satisfaction."
        by_theme = []
        
        for theme_name, winner_info in list(theme_winners.items())[:5]:
            theme_display = theme_name.replace('_', ' ').title()
            margin_desc = f"by {winner_info['margin']:.2f}" if winner_info['margin'] > 0.1 else "closely"
            by_theme.append(f"{theme_display}: {winner_info['leader_name']} leads {margin_desc}")
        
        # Build cache key
        theme_winners_data = {k: v['leader'] for k, v in theme_winners.items()}
        cache_key = get_cache_key(business_ids, theme_winners_data)
        
        # Check cache first
        cached = load_cache(cache_key)
        if cached:
            logger.info("Cache hit for comparison", cache_key=cache_key[:16])
            cached['cached'] = True
            cached['source'] = 'llm'
            return cached
        
        # Try LLM generation with fallback to deterministic
        try:
            # Build prompt text
            prompt_text = f"Compare these restaurants:\n"
            for bid in business_ids:
                prompt_text += f"\n{business_map[bid]}:\n"
                for theme_name, info in business_themes[bid].items():
                    theme_display = theme_name.replace('_', ' ').title()
                    delta_str = f" (+{info['delta']:.2f})" if info['delta'] and info['delta'] > 0 else f" ({info['delta']:.2f})" if info['delta'] else ""
                    prompt_text += f"  {theme_display}: {info['score']:.2f}{delta_str}\n"
            
            prompt_text += f"\nLeaders per theme:\n"
            for theme_name, info in list(theme_winners.items())[:5]:
                theme_display = theme_name.replace('_', ' ').title()
                prompt_text += f"  {theme_display}: {info['leader_name']} (margin: {info['margin']:.2f})\n"
            
            prompt_text += "\nGenerate a 60-90 word summary, up to 5 theme leader lines, 2 risks, and 3 opportunities. Return JSON: {summary, by_theme, risks, opportunities}"
            
            # Call Ollama
            llm_response = call_ollama(prompt_text)
            
            # Validate output
            validated, error = validate_narrative_output(llm_response)
            
            if validated and not error:
                result = {
                    'summary': validated['summary'],
                    'by_theme': validated['by_theme'],
                    'risks': validated['risks'],
                    'opportunities': validated['opportunities'],
                    'overall_leader': overall_leader_name,
                    'source': 'llm',
                    'cached': False,
                    'generated_at': datetime.now().isoformat()
                }
                
                # Save to cache
                save_cache(cache_key, result)
                logger.info("Generated narrative via LLM", cache_key=cache_key[:16])
                return result
            else:
                logger.warning("LLM validation failed, using fallback", error=error)
        except Exception as e:
            logger.error("LLM generation failed, using fallback", error=str(e))
        
        # Fallback to deterministic
        result = {
            'summary': summary,
            'by_theme': by_theme,
            'risks': [],
            'opportunities': [],
            'overall_leader': overall_leader_name,
            'source': 'fallback',
            'cached': False,
            'generated_at': datetime.now().isoformat()
        }
        
        logger.info("Returned narrative comparison", businesses=len(business_ids), source="fallback")
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error in narrative comparison", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/api/businesses/{business_id}/kpis")
async def get_business_kpis(business_id: str, period: str = "30d"):
    """Get KPIs for a business over a period"""
    db = next(get_db())
    
    try:
        business = db.query(Business).filter(Business.id == business_id).first()
        if not business:
            raise HTTPException(status_code=404, detail="Business not found")
        
        # Get trends for this business
        trends = db.query(Trend).filter(Trend.business_id == business_id).all()
        
        if not trends:
            return {
                "total_reviews": 0,
                "sentiment_score": 50,
                "avg_stars": 0,
                "deltas": {"reviews": 0, "sentiment": 0, "stars": 0},
                "sparkline": []
            }
        
        # Parse period (30d, 90d, ytd)
        from datetime import datetime, timedelta
        import pandas as pd
        
        # Parse months as YYYY-MM strings and filter
        def parse_month(month_str):
            try:
                year, month = month_str.split('-')
                return int(year), int(month)
            except:
                return 0, 0
        
        # Get latest month from trends
        latest_month = max(t.month for t in trends) if trends else "2022-01"
        latest_year, latest_mon = parse_month(latest_month)
        
        # Filter trends by period (use last N months of data)
        if period == "30d":
            # Last month
            period_trends = [t for t in trends if t.month == latest_month]
            # Prior month
            prev_month = f"{latest_year}-{latest_mon-1:02d}" if latest_mon > 1 else f"{latest_year-1}-12"
            prior_period_trends = [t for t in trends if t.month == prev_month]
        elif period == "90d":
            # Last 3 months
            months_to_include = set()
            for i in range(3):
                year = latest_year
                mon = latest_mon - i
                if mon < 1:
                    mon += 12
                    year -= 1
                months_to_include.add(f"{year}-{mon:02d}")
            period_trends = [t for t in trends if t.month in months_to_include]
            
            # Prior 3 months
            months_prior = set()
            for i in range(3, 6):
                year = latest_year
                mon = latest_mon - i
                if mon < 1:
                    mon += 12
                    year -= 1
                months_prior.add(f"{year}-{mon:02d}")
            prior_period_trends = [t for t in trends if t.month in months_prior]
        else:  # ytd
            # Current year
            period_trends = [t for t in trends if t.month.startswith(str(latest_year))]
            # Prior year
            prior_period_trends = [t for t in trends if t.month.startswith(str(latest_year - 1))]
        
        # Calculate metrics
        total_reviews = sum(t.review_count for t in period_trends)
        total_sentiment = sum(t.avg_sentiment * t.review_count for t in period_trends if t.avg_sentiment and t.review_count)
        avg_sentiment = total_sentiment / total_reviews if total_reviews > 0 else 0
        
        # Scale to 0-100
        sentiment_score = int((avg_sentiment + 1) * 50) if avg_sentiment else 50
        
        # Get avg stars from reviews
        reviews = db.query(Review).filter(Review.business_id == business_id).limit(1000).all()
        avg_stars = sum(r.stars for r in reviews) / len(reviews) if reviews else 0
        
        # Calculate deltas vs prior period
        prior_reviews = sum(t.review_count for t in prior_period_trends)
        prior_sentiment = sum(t.avg_sentiment * t.review_count for t in prior_period_trends if t.avg_sentiment and t.review_count) / prior_reviews if prior_reviews > 0 else 0
        prior_sentiment_score = int((prior_sentiment + 1) * 50) if prior_sentiment else 50
        
        deltas = {
            "reviews": total_reviews - prior_reviews,
            "sentiment": sentiment_score - prior_sentiment_score,
            "stars": 0  # Would need prior stars calculation
        }
        
        # Build sparkline data
        sparkline = sorted([{"month": t.month, "sentiment": t.avg_sentiment * 50 + 50} for t in period_trends], key=lambda x: x["month"])
        
        result = {
            "total_reviews": total_reviews,
            "sentiment_score": sentiment_score,
            "avg_stars": float(avg_stars),
            "deltas": deltas,
            "sparkline": sparkline,
            "mentions_analyzed": total_reviews
        }
        
        logger.info("Returned KPIs", business_id=business_id, period=period)
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching KPIs", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/api/businesses/{business_id}/quotes")
async def get_business_quotes(business_id: str, period: str = "30d"):
    """Get representative quotes by theme for a period"""
    db = next(get_db())
    
    try:
        business = db.query(Business).filter(Business.id == business_id).first()
        if not business:
            raise HTTPException(status_code=404, detail="Business not found")
        
        # Load cached quotes file
        quotes_file = project_root / "data" / "keywords_quotes" / f"{business_id}_quotes.json"
        
        if not quotes_file.exists():
            return {"quotes_by_theme": {}}
        
        with open(quotes_file, 'r') as f:
            quotes_data = json.load(f)
        
        # Build quotes by theme
        quotes_by_theme = {}
        for theme_name, theme_quotes in quotes_data.items():
            pos_quotes = [q[:160] for q in theme_quotes.get('positive', [])[:2]]
            neg_quotes = [q[:160] for q in theme_quotes.get('negative', [])[:2]]
            
            if pos_quotes or neg_quotes:
                quotes_by_theme[theme_name] = {
                    "positive": pos_quotes,
                    "negative": neg_quotes
                }
        
        result = {"quotes_by_theme": quotes_by_theme}
        
        logger.info("Returned quotes", business_id=business_id, themes=len(quotes_by_theme))
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching quotes", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.post("/api/businesses/{business_id}/refresh")
async def refresh_business(business_id: str, period: str = "30d"):
    """Refresh business analysis for a specific period"""
    from backend.refresh_handler import run_refresh_transaction
    
    logger.info("Refresh requested", business_id=business_id, period=period)
    
    db = next(get_db())
    try:
        # Check if business exists
        business = db.query(Business).filter(Business.id == business_id).first()
        if not business:
            raise HTTPException(status_code=404, detail="Business not found")
        
        # Validate period
        if period not in ["30d", "90d", "ytd"]:
            raise HTTPException(status_code=400, detail="Invalid period. Must be 30d, 90d, or ytd")
        
        # Run refresh in transaction
        result = run_refresh_transaction(db, business_id, period)
        
        if not result.get('success'):
            raise HTTPException(status_code=500, detail=result.get('error', 'Refresh failed'))
        
        logger.info("Refresh completed successfully", business_id=business_id)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error in refresh endpoint", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

# In-memory cache for query results
_query_cache: Dict[str, Dict[str, Any]] = {}
_cache_ttl = 24 * 60 * 60  # 24 hours in seconds

def _get_cache_key(business_id: str, start_date: str, end_date: str, keywords: List[str]) -> str:
    """Generate cache key from query parameters"""
    sorted_keywords = sorted([k.lower().strip() for k in keywords])
    key_str = f"{business_id}|{start_date}|{end_date}|{','.join(sorted_keywords)}"
    return hashlib.sha256(key_str.encode()).hexdigest()

def _is_cache_valid(cache_entry: Dict[str, Any]) -> bool:
    """Check if cache entry is still valid"""
    if 'timestamp' not in cache_entry:
        return False
    age = (datetime.now() - cache_entry['timestamp']).total_seconds()
    return age < _cache_ttl

# VADER sentiment analyzer (reuse for keyword queries)
_vader_analyzer = SentimentIntensityAnalyzer()

def _match_keyword_in_text(text: str, keyword: str) -> bool:
    """Match keyword using exact phrase first, then fuzzy >= 85"""
    text_lower = text.lower()
    keyword_lower = keyword.lower().strip()
    
    # Exact phrase match
    if keyword_lower in text_lower:
        return True
    
    # Fuzzy match with rapidfuzz
    # Try matching against words and phrases in text
    words = text_lower.split()
    for i in range(len(words)):
        for j in range(i + 1, min(i + 5, len(words) + 1)):  # Check up to 4-word phrases
            phrase = ' '.join(words[i:j])
            ratio = fuzz.partial_ratio(keyword_lower, phrase)
            if ratio >= 85:
                return True
    
    return False

@app.get("/api/businesses/{business_id}/date-range")
async def get_business_date_range(business_id: str):
    """Get available date range for a business's reviews"""
    db = next(get_db())
    
    try:
        from sqlalchemy import func
        
        # Get min and max dates for this business
        result = db.query(
            func.min(Review.date).label('min_date'),
            func.max(Review.date).label('max_date'),
            func.count(Review.id).label('total_reviews')
        ).filter(Review.business_id == business_id).first()
        
        if not result or result.total_reviews == 0:
            raise HTTPException(status_code=404, detail="No reviews found for this business")
        
        return {
            "min_date": result.min_date.isoformat() if result.min_date else None,
            "max_date": result.max_date.isoformat() if result.max_date else None,
            "total_reviews": result.total_reviews
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching date range", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/api/search/businesses")
async def search_businesses(q: str = ""):
    """Search businesses by name (returns top 10 matches)"""
    db = next(get_db())
    
    try:
        if not q or len(q.strip()) < 1:
            # Return all businesses if no query
            businesses = db.query(Business).order_by(Business.review_count.desc()).limit(10).all()
        else:
            # Search by name (case-insensitive LIKE - SQLite uses lower() for case-insensitive)
            search_term = f"%{q.strip().lower()}%"
            businesses = db.query(Business).filter(
                func.lower(Business.name).like(search_term)
            ).order_by(Business.review_count.desc()).limit(10).all()
        
        results = [
            {
                "id": b.id,
                "name": b.name,
                "city": b.city or "",
                "review_count": b.review_count
            }
            for b in businesses
        ]
        
        logger.info("Business search", query=q, results=len(results))
        return results
    
    except Exception as e:
        logger.error("Error searching businesses", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

class QueryRequest(BaseModel):
    business_id: str
    start_date: str  # ISO format: YYYY-MM-DD
    end_date: str    # ISO format: YYYY-MM-DD
    keywords: TypingList[str]  # Max 10 keywords

@app.post("/api/query")
async def query_keyword_analytics(request: QueryRequest):
    """
    Query keyword analytics for a business over a date range
    
    Filters reviews by business_id, date range, and keywords.
    Returns KPIs, time series, by-keyword stats, quotes, and AI summary.
    """
    db = next(get_db())
    
    try:
        # Validate keywords (max 10)
        if len(request.keywords) > 10:
            raise HTTPException(status_code=400, detail="Maximum 10 keywords allowed")
        
        if len(request.keywords) == 0:
            raise HTTPException(status_code=400, detail="At least one keyword required")
        
        # Check cache
        cache_key = _get_cache_key(request.business_id, request.start_date, request.end_date, request.keywords)
        if cache_key in _query_cache and _is_cache_valid(_query_cache[cache_key]):
            logger.info("Cache hit", cache_key=cache_key[:16])
            return _query_cache[cache_key]['data']
        
        # Validate business exists
        business = db.query(Business).filter(Business.id == request.business_id).first()
        if not business:
            raise HTTPException(status_code=404, detail="Business not found")
        
        # Parse dates
        try:
            start_dt = datetime.strptime(request.start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(request.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        
        if start_dt > end_dt:
            raise HTTPException(status_code=400, detail="start_date must be before end_date")
        
        # Filter reviews by business and date range
        from sqlalchemy import and_
        reviews = db.query(Review).filter(
            and_(
                Review.business_id == request.business_id,
                Review.date >= start_dt,
                Review.date <= end_dt
            )
        ).all()
        
        if not reviews:
            return {
                "insufficient_data": True,
                "message": "No reviews found in the specified date range",
                "matched_reviews": 0
            }
        
        # Match reviews by keywords
        matched_reviews = []
        for review in reviews:
            review_text = (review.text or "").lower()
            for keyword in request.keywords:
                if _match_keyword_in_text(review_text, keyword):
                    matched_reviews.append(review)
                    break  # Review matches if any keyword matches
        
        if len(matched_reviews) < 25:
            # Return partial results without LLM
            return {
                "insufficient_data": True,
                "message": f"Only {len(matched_reviews)} reviews matched. Need at least 25 for AI summary.",
                "matched_reviews": len(matched_reviews),
                "total_reviews": len(reviews)
            }
        
        # Compute sentiment for matched reviews
        sentiments = []
        stars = []
        for review in matched_reviews:
            text = review.text or ""
            if text:
                vs = _vader_analyzer.polarity_scores(text)
                sentiments.append(vs['compound'])
            if review.stars:
                stars.append(review.stars)
        
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0.0
        sentiment_score = int((avg_sentiment + 1) * 50)  # Convert -1..1 to 0..100
        avg_stars = sum(stars) / len(stars) if stars else 0.0
        
        # Compute prior period for deltas
        # First, get the actual min date for this business to avoid "date out of range" errors
        from sqlalchemy import func
        min_date_result = db.query(func.min(Review.date)).filter(
            Review.business_id == request.business_id
        ).scalar()
        
        period_days = (end_dt - start_dt).days
        prior_start = start_dt - timedelta(days=period_days)
        prior_end = start_dt - timedelta(days=1)
        
        # Only calculate prior period if we have valid dates
        if min_date_result and prior_start >= min_date_result and prior_end >= min_date_result:
            prior_reviews = db.query(Review).filter(
                and_(
                    Review.business_id == request.business_id,
                    Review.date >= prior_start,
                    Review.date <= prior_end
                )
            ).all()
        else:
            prior_reviews = []
        
        prior_matched = []
        for review in prior_reviews:
            review_text = (review.text or "").lower()
            for keyword in request.keywords:
                if _match_keyword_in_text(review_text, keyword):
                    prior_matched.append(review)
                    break
        
        prior_sentiments = []
        prior_stars = []
        for review in prior_matched:
            text = review.text or ""
            if text:
                vs = _vader_analyzer.polarity_scores(text)
                prior_sentiments.append(vs['compound'])
            if review.stars:
                prior_stars.append(review.stars)
        
        prior_avg_sentiment = sum(prior_sentiments) / len(prior_sentiments) if prior_sentiments else 0.0
        prior_sentiment_score = int((prior_avg_sentiment + 1) * 50)
        prior_avg_stars = sum(prior_stars) / len(prior_stars) if prior_stars else 0.0
        
        deltas = {
            "reviews": len(matched_reviews) - len(prior_matched),
            "sentiment": sentiment_score - prior_sentiment_score,
            "stars": round(avg_stars - prior_avg_stars, 2)
        }
        
        # Generate time series (weekly for <=90d, monthly for >90d)
        period_days = (end_dt - start_dt).days
        if period_days <= 90:
            # Weekly buckets
            buckets = []
            current = start_dt
            while current <= end_dt:
                week_end = min(current + timedelta(days=6), end_dt)
                week_reviews = [r for r in matched_reviews if current <= r.date <= week_end]
                if week_reviews:
                    week_sentiments = []
                    for r in week_reviews:
                        if r.text:
                            vs = _vader_analyzer.polarity_scores(r.text)
                            week_sentiments.append(vs['compound'])
                    avg_sent = sum(week_sentiments) / len(week_sentiments) if week_sentiments else 0.0
                    buckets.append({
                        "bucket": current.strftime("%Y-%m-%d"),
                        "hits": len(week_reviews),
                        "avg_sentiment": round(avg_sent, 3)
                    })
                current = week_end + timedelta(days=1)
            time_series = buckets
        else:
            # Monthly buckets
            buckets = []
            current = start_dt.replace(day=1)
            while current <= end_dt:
                # Get last day of month
                if current.month == 12:
                    month_end = current.replace(year=current.year + 1, month=1, day=1) - timedelta(days=1)
                else:
                    month_end = current.replace(month=current.month + 1, day=1) - timedelta(days=1)
                month_end = min(month_end, end_dt)
                
                month_reviews = [r for r in matched_reviews if current <= r.date <= month_end]
                if month_reviews:
                    month_sentiments = []
                    for r in month_reviews:
                        if r.text:
                            vs = _vader_analyzer.polarity_scores(r.text)
                            month_sentiments.append(vs['compound'])
                    avg_sent = sum(month_sentiments) / len(month_sentiments) if month_sentiments else 0.0
                    buckets.append({
                        "bucket": current.strftime("%Y-%m"),
                        "hits": len(month_reviews),
                        "avg_sentiment": round(avg_sent, 3)
                    })
                # Move to next month
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    current = current.replace(month=current.month + 1, day=1)
            time_series = buckets
        
        # By keyword stats
        by_keyword = []
        total_keyword_hits = 0
        keyword_hits_map = {}
        
        for keyword in request.keywords:
            keyword_matched = [r for r in matched_reviews if _match_keyword_in_text((r.text or "").lower(), keyword)]
            keyword_sentiments = []
            for r in keyword_matched:
                if r.text:
                    vs = _vader_analyzer.polarity_scores(r.text)
                    keyword_sentiments.append(vs['compound'])
            avg_kw_sent = sum(keyword_sentiments) / len(keyword_sentiments) if keyword_sentiments else 0.0
            hits = len(keyword_matched)
            keyword_hits_map[keyword] = hits
            total_keyword_hits += hits
            
            by_keyword.append({
                "term": keyword,
                "hits": hits,
                "avg_sentiment": round(avg_kw_sent, 3)
            })
        
        # Quotes (up to 2 positive & 2 negative per keyword) - with deduplication
        quotes_by_keyword = {}
        used_review_ids = set()  # Track used reviews to avoid duplicates
        
        for keyword in request.keywords:
            keyword_matched = [r for r in matched_reviews if _match_keyword_in_text((r.text or "").lower(), keyword)]
            positive_quotes = []
            negative_quotes = []
            
            for review in keyword_matched:
                # Skip if this review was already used for another keyword
                if review.id in used_review_ids:
                    continue
                    
                if not review.text:
                    continue
                    
                vs = _vader_analyzer.polarity_scores(review.text)
                text = review.text.strip()
                
                # Extract context around keyword
                if len(text) > 160:
                    text_lower = text.lower()
                    kw_pos = text_lower.find(keyword.lower())
                    if kw_pos >= 0:
                        start = max(0, kw_pos - 60)
                        end = min(len(text), kw_pos + len(keyword) + 60)
                        text = text[start:end]
                        if start > 0:
                            text = "..." + text
                        if end < len(review.text):
                            text = text + "..."
                    else:
                        text = text[:160] + "..."
                
                quote_entry = {
                    "text": text,
                    "review_id": review.id,
                    "sentiment": vs['compound']
                }
                
                # Proper sentiment classification
                if vs['compound'] >= 0.4 and len(positive_quotes) < 2:
                    positive_quotes.append(quote_entry)
                    used_review_ids.add(review.id)
                elif vs['compound'] <= -0.2 and len(negative_quotes) < 2:
                    negative_quotes.append(quote_entry)
                    used_review_ids.add(review.id)
                
                if len(positive_quotes) >= 2 and len(negative_quotes) >= 2:
                    break
            
            # If we don't have enough quotes, try unused reviews (but still respect sentiment)
            if len(positive_quotes) < 2 or len(negative_quotes) < 2:
                for review in keyword_matched:
                    if review.id in used_review_ids:
                        continue
                    if not review.text:
                        continue
                    
                    vs = _vader_analyzer.polarity_scores(review.text)
                    text = review.text.strip()
                    if len(text) > 160:
                        text = text[:160] + "..."
                    
                    if vs['compound'] >= 0.4 and len(positive_quotes) < 2:
                        positive_quotes.append({"text": text, "review_id": review.id, "sentiment": vs['compound']})
                        used_review_ids.add(review.id)
                    elif vs['compound'] <= -0.2 and len(negative_quotes) < 2:
                        negative_quotes.append({"text": text, "review_id": review.id, "sentiment": vs['compound']})
                        used_review_ids.add(review.id)
                    
                    if len(positive_quotes) >= 2 and len(negative_quotes) >= 2:
                        break
            
            quotes_by_keyword[keyword] = {
                "positive": [q["text"] for q in positive_quotes],
                "negative": [q["text"] for q in negative_quotes]
            }
        
        # Generate sparkline (last 7 buckets or all if < 7)
        sparkline = [b["avg_sentiment"] for b in time_series[-7:]]
        
        # Prepare KPIs
        kpis = {
            "matched_reviews": len(matched_reviews),
            "sentiment_score": sentiment_score,
            "avg_stars": round(avg_stars, 2),
            "deltas": deltas,
            "sparkline": sparkline
        }
        
        # LLM Summary (call Ollama with timeout)
        summary_source = "fallback"
        summary_data = {
            "love": [],
            "improve": [],
            "recommendations": []
        }
        
        try:
            # Prepare detailed prompt for Ollama with sample quotes
            sample_quotes_text = ""
            for kw_stat in by_keyword[:3]:  # Top 3 keywords
                kw_quotes = quotes_by_keyword.get(kw_stat['term'], {})
                if kw_quotes.get('positive'):
                    sample_quotes_text += f"\nPositive quote about '{kw_stat['term']}': {kw_quotes['positive'][0][:100]}..."
                if kw_quotes.get('negative'):
                    sample_quotes_text += f"\nNegative quote about '{kw_stat['term']}': {kw_quotes['negative'][0][:100]}..."
            
            prompt = f"""You are BizVista AI, an expert at analyzing restaurant customer feedback.

Restaurant: {business.name}
Analysis Period: {request.start_date} to {request.end_date}
Total Reviews Analyzed: {len(matched_reviews)}

Keywords Analyzed:
"""
            for kw_stat in by_keyword:
                sentiment_pct = int((kw_stat['avg_sentiment'] + 1) * 50)
                prompt += f"- '{kw_stat['term']}': {kw_stat['hits']} mentions, {sentiment_pct}% positive sentiment\n"
            
            prompt += f"""
Sample Customer Quotes:{sample_quotes_text}

Overall Sentiment Score: {sentiment_score}% (0-100 scale)
Average Star Rating: {avg_stars:.1f}/5.0

Generate actionable insights in JSON format:
{{
  "love": [3-5 specific things customers praise, reference actual keywords and numbers],
  "improve": [3-5 specific areas needing attention, reference keywords with negative sentiment],
  "recommendations": [3 concrete, actionable recommendations based on the data]
}}

Rules:
- Be specific: mention keyword names, sentiment scores, mention counts
- Reference actual customer feedback patterns
- Recommendations must be actionable (not generic)
- Total max 200 words across all fields
- No generic phrases like "continue monitoring" or "focus on keywords"
"""
            
            start_time = datetime.now()
            llm_response = requests.post(
                OLLAMA_URL,
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "temperature": 0.4,
                    "num_ctx": 2048,
                    "num_predict": 400,
                    "top_k": 40,
                    "top_p": 0.9,
                    "stream": False
                },
                timeout=60
            )
            elapsed = (datetime.now() - start_time).total_seconds()
            
            if elapsed <= 60 and llm_response.status_code == 200:
                llm_data = llm_response.json()
                llm_text = llm_data.get('response', '')
                
                # Try to extract JSON - improved regex to handle nested structures
                import re
                # Try to find JSON block (more flexible pattern)
                json_patterns = [
                    r'\{[^{}]*(?:"love"|"improve"|"recommendations")[^{}]*\}',  # Simple
                    r'\{[^{}]*"love"[^{}]*"improve"[^{}]*"recommendations"[^{}]*\}',  # All three
                    r'\{.*?"love".*?"improve".*?"recommendations".*?\}',  # With any content
                ]
                
                for pattern in json_patterns:
                    json_match = re.search(pattern, llm_text, re.DOTALL | re.IGNORECASE)
                    if json_match:
                        try:
                            parsed = json.loads(json_match.group())
                            # Validate structure and ensure all items are strings
                            if 'love' in parsed and 'improve' in parsed and 'recommendations' in parsed:
                                if isinstance(parsed['love'], list) and isinstance(parsed['improve'], list) and isinstance(parsed['recommendations'], list):
                                    # Convert all items to strings (handle objects/dicts)
                                    def ensure_strings(arr):
                                        result = []
                                        for item in arr:
                                            if isinstance(item, str):
                                                result.append(item)
                                            elif isinstance(item, dict):
                                                # If it's a dict, try to extract meaningful text
                                                result.append(str(item.get('text', item.get('message', str(item)))))
                                            else:
                                                result.append(str(item))
                                        return result
                                    
                                    summary_data = {
                                        'love': ensure_strings(parsed['love']),
                                        'improve': ensure_strings(parsed['improve']),
                                        'recommendations': ensure_strings(parsed['recommendations'])
                                    }
                                    summary_source = "llm"
                                    break
                        except:
                            continue
            
        except Exception as e:
            logger.warning("LLM generation failed, using fallback", error=str(e))
        
        # Fallback if LLM didn't work - generate intelligent rule-based summary
        if summary_source == "fallback":
            # Generate love points from positive keywords
            positive_keywords = [kw for kw in by_keyword if kw['avg_sentiment'] > 0.1]
            positive_keywords.sort(key=lambda x: x['hits'], reverse=True)
            
            for kw_stat in positive_keywords[:5]:
                sentiment_pct = int((kw_stat['avg_sentiment'] + 1) * 50)
                summary_data['love'].append(f"'{kw_stat['term']}' mentioned {kw_stat['hits']} times with {sentiment_pct}% positive sentiment")
            
            # Generate improve points from negative keywords
            negative_keywords = [kw for kw in by_keyword if kw['avg_sentiment'] < -0.1]
            negative_keywords.sort(key=lambda x: x['hits'], reverse=True)
            
            for kw_stat in negative_keywords[:5]:
                sentiment_pct = int((kw_stat['avg_sentiment'] + 1) * 50)
                summary_data['improve'].append(f"'{kw_stat['term']}' mentioned {kw_stat['hits']} times with {sentiment_pct}% positive sentiment - needs attention")
            
            # Generate recommendations based on data patterns
            if positive_keywords:
                top_positive = positive_keywords[0]
                summary_data['recommendations'].append(f"Leverage strength in '{top_positive['term']}' ({top_positive['hits']} mentions, {int((top_positive['avg_sentiment'] + 1) * 50)}% positive)")
            
            if negative_keywords:
                top_negative = negative_keywords[0]
                summary_data['recommendations'].append(f"Address concerns about '{top_negative['term']}' ({top_negative['hits']} mentions, {int((top_negative['avg_sentiment'] + 1) * 50)}% positive)")
            
            # Overall recommendation based on sentiment
            if sentiment_score >= 70:
                summary_data['recommendations'].append(f"Maintain high satisfaction (overall {sentiment_score}% positive sentiment)")
            elif sentiment_score < 50:
                summary_data['recommendations'].append(f"Focus on improving overall experience (currently {sentiment_score}% positive sentiment)")
            else:
                summary_data['recommendations'].append(f"Continue improving to reach higher satisfaction (currently {sentiment_score}% positive sentiment)")
            
            # Pad if needed (but avoid generic repeats)
            if len(summary_data['love']) == 0:
                summary_data['love'].append(f"Overall positive sentiment: {sentiment_score}% across {len(matched_reviews)} reviews")
            if len(summary_data['improve']) == 0:
                if sentiment_score < 60:
                    summary_data['improve'].append(f"Overall sentiment below target ({sentiment_score}% - aim for 70%+)")
                else:
                    summary_data['improve'].append("Monitor for emerging negative trends")
            if len(summary_data['recommendations']) < 3:
                summary_data['recommendations'].append(f"Analyze {len(matched_reviews)} reviews for actionable patterns")
        
        # Ensure summary arrays contain only strings (final safety check)
        def final_string_cleanup(arr):
            return [str(item) if not isinstance(item, str) else item for item in arr]
        
        summary_data = {
            'love': final_string_cleanup(summary_data.get('love', [])),
            'improve': final_string_cleanup(summary_data.get('improve', [])),
            'recommendations': final_string_cleanup(summary_data.get('recommendations', []))
        }
        
        # Build response
        response_data = {
            "kpis": kpis,
            "time_series": time_series,
            "by_keyword": by_keyword,
            "quotes_by_keyword": quotes_by_keyword,
            "summary": summary_data,
            "summary_source": summary_source,
            "share_of_voice": {
                keyword: round((hits / total_keyword_hits * 100) if total_keyword_hits > 0 else 0, 1)
                for keyword, hits in keyword_hits_map.items()
            },
            "generated_at": datetime.now().isoformat()
        }
        
        # Cache result
        _query_cache[cache_key] = {
            "data": response_data,
            "timestamp": datetime.now()
        }
        
        logger.info("Query completed", 
                   business_id=request.business_id,
                   keywords=len(request.keywords),
                   matched=len(matched_reviews),
                   source=summary_source)
        
        return response_data
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error in query endpoint", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/")
async def root():
    """Health check"""
    return {"status": "ok", "message": "BizVista AI API v1"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=4174)
