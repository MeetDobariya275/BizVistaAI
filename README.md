# BizVista AI

An AI-powered restaurant analytics dashboard built on the Yelp Open Dataset. Analyze customer feedback through sentiment analysis, theme categorization, and AI-generated insights.

## Tech Stack

- **Frontend**: Next.js, React, Tailwind CSS, Recharts, TanStack Query
- **Backend**: FastAPI, SQLAlchemy, SQLite
- **NLP**: VADER sentiment, TF-IDF keywords, rapidfuzz theme matching
- **LLM**: Ollama (phi3:mini) for AI insights
- **Language**: Python 3.9+, Node.js 18+

## Prerequisites

- Python 3.9+ with pip
- Node.js 18+ with npm/pnpm
- Ollama installed locally ([download](https://ollama.ai))
- Yelp Open Dataset (instructions below)

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd BizVistaAI
```

### 2. Download Yelp Dataset

Download the Yelp Open Dataset from [here](https://www.yelp.com/dataset).

Extract and place the following files into the `data/` directory:
- `yelp_academic_dataset_business.json`
- `yelp_academic_dataset_review.json`

**Note**: The dataset is ~8GB total. Only the business and review JSON files are required.

### 3. Backend Setup

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # On macOS/Linux
# OR
.venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt

# The database will be created automatically on first run
```

### 4. Frontend Setup

```bash
cd frontend
npm install
# OR
pnpm install
```

### 5. Start Ollama (LLM Service)

```bash
# Start Ollama service locally
ollama serve

# Pull the required model (if not already installed)
ollama pull phi3:mini
```

### 6. Run the Application

**Terminal 1 - Backend:**
```bash
# From project root
source .venv/bin/activate
cd backend
python api.py
```

Backend runs on `http://localhost:4174`

**Terminal 2 - Frontend:**
```bash
cd frontend
npm run dev
# OR
pnpm dev
```

Frontend runs on `http://localhost:4173`

### 7. Access the Dashboard

- Home: `http://localhost:4173`
- Dashboard: `http://localhost:4173/dashboard`
- Restaurant Overview: `http://localhost:4173/biz/{id}`
- Trends: `http://localhost:4173/biz/{id}/trends`
- Compare: `http://localhost:4173/compare`

## Project Structure

```
BizVistaAI/
├── backend/          # FastAPI backend
│   ├── api.py       # Main API endpoints
│   ├── refresh_handler.py
│   └── ...          # ETL scripts
├── frontend/         # Next.js frontend
│   ├── app/         # Pages and routes
│   ├── lib/         # API clients and schemas
│   └── ...
├── data/            # Yelp dataset and processed files
└── bizvista.db      # SQLite database
```

## Features

- **KPI Dashboard**: Total reviews, sentiment score, star ratings with period comparisons
- **Performance Tracker**: Time-series sentiment visualization
- **Theme Analysis**: 8 fixed categories (Food Quality, Service, Speed, Ambiance, etc.)
- **AI Insights**: LLM-generated recommendations and analysis
- **Task Monitoring**: Action items with priority and effort scoring
- **Restaurant Comparison**: Narrative-based insights for up to 3 restaurants
- **Dynamic Refresh**: Re-analyze reviews for any time period

## API Endpoints

- `GET /api/businesses` - List all restaurants
- `GET /api/businesses/{id}/overview` - Get overview data
- `GET /api/businesses/{id}/trends` - Get monthly trends
- `GET /api/businesses/{id}/kpis` - Get KPIs for period
- `GET /api/businesses/{id}/quotes` - Get representative quotes
- `GET /api/compare-narrative` - Compare restaurants narratively
- `POST /api/businesses/{id}/refresh` - Re-run analysis for period

## Development Notes

- Database is SQLite (`bizvista.db` in project root)
- LLM insights cached in `data/cache/`
- Period-aware: supports 30d, 90d, YTD filtering
- All review analysis is performed locally (no external API calls)

## Troubleshooting

**Backend won't start:**
- Ensure virtual environment is activated
- Check that all Python dependencies are installed
- Verify `bizvista.db` exists or create it by running the ETL scripts

**Frontend shows "Loading..." forever:**
- Ensure backend is running on `http://localhost:4174`
- Check browser console for API errors
- Verify CORS settings in backend

**LLM insights are generic:**
- Check if Ollama is running: `curl http://localhost:11434/api/tags`
- Ensure `phi3:mini` model is pulled: `ollama pull phi3:mini`
- Check `data/cache/` for insight files

**No data showing:**
- Verify Yelp dataset files are in `data/` directory
- Run the ETL pipeline to process reviews
- Check database for populated tables

## License

MIT

## Dataset License

Yelp Open Dataset License - See `data/Dataset_User_Agreement.pdf`
