# Dublin Bikes: Surplus/Deficit Optimization Dashboard

> A full end-to-end data engineering project that automatically ingests, transforms, and visualizes Dublin Bikes station data — identifying surplus and deficit stations in real time and generating optimized rebalancing routes.

**Live Dashboard:** https://dublinbikessurplusdeficit.streamlit.app/

---

## What it does

Managing bike-sharing station balance is a logistical challenge — some stations overflow with bikes while others run empty, frustrating commuters. The Dublin Bikes Surplus/Deficit Optimization Dashboard solves this by automatically ingesting both historical and real-time station data, processing it through a cloud ETL pipeline, and surfacing actionable insights on an interactive Streamlit dashboard.

The system answers three key questions:
- Which stations are currently oversupplied or undersupplied?
- When during the day do imbalances peak?
- What is the most efficient set of rebalancing routes to fix them?

---

## Behind the build

**Stack:** Python, Streamlit, Plotly, SQLAlchemy, Supabase (PostgreSQL), MongoDB Atlas, GitHub Actions, scikit-learn, SciPy

**Why these choices?**

Supabase was chosen as the primary analytical store because it provides a fully managed PostgreSQL instance with a generous free tier, a connection pooler (Supavisor) that handles Streamlit Cloud's dynamic IP addresses, and SSL enforcement out of the box. MongoDB Atlas was used as a raw snapshot store for the real-time GBFS API responses — its document model is a natural fit for semi-structured JSON payloads before they are flattened and cleaned into relational tables.

GitHub Actions was selected as the orchestration layer instead of a dedicated tool like Airflow or Prefect. Since the pipeline runs on a fixed hourly schedule and the logic is straightforward, GitHub Actions provides zero-infrastructure scheduling with native secret management and a free execution tier — a simpler and more cost-effective choice for this scale.

For the optimization layer, SciPy's `linear_sum_assignment` (the Hungarian algorithm) was used to compute the globally optimal set of bike redistribution routes between surplus and deficit stations, minimising total travel distance. DBSCAN clustering from scikit-learn was applied to group geographically close stations into service zones.

---

## What was hard

**Connecting to cloud databases from Streamlit Cloud**
Streamlit Cloud runs on ephemeral infrastructure with dynamic IP addresses, which meant neither Supabase's direct connection (port 5432) nor a standard MongoDB Atlas IP whitelist worked out of the box. The fix required switching Supabase to its Transaction Pooler endpoint (port 6543) and setting MongoDB Atlas to allow connections from anywhere (`0.0.0.0/0`). Getting SSL enforced correctly through SQLAlchemy's `connect_args` also required careful configuration.

**Inconsistent GBFS API responses**
The real-time Cyclocity GBFS API returns station status and station information as two separate endpoints that must be joined on `station_id`. Field names were inconsistent across response versions (`lat` vs `latitude`, `lon` vs `longitude`), requiring defensive extraction logic in the transform layer to avoid silent null injection into the database.

---

## Results / Impact

- **Automated pipeline** running every hour via GitHub Actions — zero manual data entry required after initial setup
- **Dual-database architecture** cleanly separating raw JSON snapshots (MongoDB) from structured analytical data (PostgreSQL), making the system easy to debug and extend
- **Interactive rebalancing routes** computed in real time using the Hungarian algorithm across the top surplus and deficit stations, with Haversine distance minimisation
- **Full observability** — every pipeline run is logged in GitHub Actions with step-level output, making failures easy to diagnose

---

## Dashboard Features

| Feature | Description |
|---------|-------------|
| KPI Cards | Peak utilization rate, surplus/deficit station counts, average imbalance score |
| Utilization Heatmap | Station utilization by hour, filterable via sidebar time range |
| Station Cluster Map | DBSCAN clustering of stations by proximity (500m radius) |
| Surplus vs Deficit Map | Color-coded live map — green surplus, red deficit, blue balanced |
| Top 5 Tables | Highest imbalance surplus and deficit stations with scores |
| Rebalancing Routes | Optimal bike redistribution routes rendered on an interactive map |

---

## Architecture

```
Data Sources
    │
    ├── Historical CSV       (data.smartdublin.ie — September 2024)
    └── Real-time GBFS API   (api.cyclocity.fr — 20 snapshots per run)
          │
          ▼
    ETL Pipeline  ──  GitHub Actions (hourly cron)
          │
          ├── Extract    fetch CSV + GBFS status & info snapshots
          ├── Transform  clean, validate, join, compute utilization & imbalance
          └── Load       MongoDB Atlas (raw JSON) → Supabase PostgreSQL (clean)
                │
                ▼
    Streamlit Dashboard  (visualization.py)
```

---

## Project Structure

```
dublinBikesOptimization/
├── .github/
│   └── workflows/
│       └── etl.yml          # Scheduled ETL pipeline (runs hourly)
├── etl/
│   ├── config.py            # Loads env variables — no hardcoded secrets
│   ├── extract.py           # Fetches CSV and GBFS API snapshots
│   ├── transform.py         # Cleans and enriches raw data
│   ├── load.py              # Loads clean data into PostgreSQL
│   ├── schema.py            # Creates PostgreSQL tables (idempotent)
│   └── pipeline.py          # Orchestrates the full ETL flow
├── visualization.py         # Streamlit dashboard
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Local Setup & Installation

### Prerequisites
- Python 3.10+
- A [Supabase](https://supabase.com) project (free tier)
- A [MongoDB Atlas](https://cloud.mongodb.com) cluster (free tier)

### 1. Clone the repo
```bash
git clone https://github.com/car-lin/dublinBikesOptimization.git
cd dublinBikesOptimization
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set up secrets

Create `.streamlit/secrets.toml` (this file is gitignored — never commit it):
```toml
POSTGRES_URI = "postgresql+psycopg2://postgres.YOUR_REF:PASSWORD@aws-0-eu-west-1.pooler.supabase.com:6543/postgres"
MONGO_URI    = "mongodb+srv://YOUR_USER:PASSWORD@YOUR_CLUSTER.mongodb.net/?appName=YOUR_APP"
```

> **Important:** Use the Supabase **Transaction Pooler URL** (port `6543`), not the direct connection URL (port `5432`). The direct connection does not work on Streamlit Cloud or GitHub Actions.

### 4. Run the ETL pipeline
```bash
python etl/pipeline.py
```

### 5. Launch the dashboard
```bash
streamlit run visualization.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## Deployment

### Streamlit Cloud
1. Push to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) → New app
3. Set main file to `visualization.py`
4. Go to **App Settings → Secrets** and paste:
```toml
POSTGRES_URI = "your-pooler-url"
MONGO_URI    = "your-mongo-uri"
```

### GitHub Actions (ETL scheduling)
Add these under **Repo → Settings → Secrets and variables → Actions**:
- `POSTGRES_URI`
- `MONGO_URI`

The pipeline runs automatically every hour. You can also trigger it manually from the **Actions** tab.

---

## Data Sources

| Source | Description |
|--------|-------------|
| [data.smartdublin.ie](https://data.smartdublin.ie) | Historical Dublin Bikes CSV (September 2024) |
| [Cyclocity GBFS API](https://api.cyclocity.fr/contracts/dublin/gbfs/) | Real-time station status and information |

---

## Author

**car-lin** — [github.com/car-lin](https://github.com/car-lin)
