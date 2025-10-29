#!/usr/bin/env python3
"""
FastAPI Backend - v1 (read-only)
Endpoints for querying business data from SQLite database
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Float, Text, ForeignKey, Date, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
import json
import hashlib
import requests
from datetime import datetime
from pathlib import Path
import structlog

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

@app.get("/api/compare")
async def compare_businesses(ids: str):
    """Compare multiple businesses by themes"""
    db = next(get_db())
    
    try:
        # Parse business IDs
        business_ids = [bid.strip() for bid in ids.split(',')]
        
        # Get all themes for these businesses
        themes = db.query(Theme).filter(Theme.business_id.in_(business_ids)).all()
        
        # Get business info
        businesses = db.query(Business).filter(Business.id.in_(business_ids)).all()
        business_map = {b.id: b.name for b in businesses}
        
        # Get unique theme names
        all_themes = sorted(set(t.theme for t in themes))
        
        # Build scores matrix
        scores = []
        for business_id in business_ids:
            business_themes = {t.theme: t.score for t in themes if t.business_id == business_id}
            
            score_row = {
                "business_id": business_id,
                "name": business_map.get(business_id, "Unknown")
            }
            
            for theme in all_themes:
                score_row[theme] = business_themes.get(theme, 0.0)
            
            scores.append(score_row)
        
        result = {
            "themes": all_themes,
            "scores": scores
        }
        
        logger.info("Returned comparison", businesses=len(business_ids))
        return result
    
    except Exception as e:
        logger.error("Error comparing businesses", error=str(e))
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

@app.get("/")
async def root():
    """Health check"""
    return {"status": "ok", "message": "BizVista AI API v1"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=4174)
