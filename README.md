# Uber Analytics — ETL · PostgreSQL · Power BI

End-to-end data analytics pipeline for Uber trip data.

# Uber Analytics — ETL · PostgreSQL · Power BI

## Dashboard Preview
![Uber Analytics Dashboard](uber_analytics.pdf)

## Stack
- Python 3.12 (pandas, SQLAlchemy, psycopg2)
- PostgreSQL 18.4
- Power BI Desktop (DirectQuery)

## Architecture
CSV → Extract → Transform → Load → PostgreSQL → 5 Views → Power BI

## Project Structure
Uber-analytics/
├── pipeline.py         # ETL orchestrator
├── etl/
│   ├── extractor.py    # CSV / API ingestion
│   ├── transformer.py  # Clean & enrich
│   └── loader.py       # PostgreSQL writer
├── warehouse/
│   ├── schema.sql      # Star schema DDL
│   └── views.sql       # 5 analytical views
├── powerbi/
│   └── measures.dax    # 30+ DAX measures
├── assets/
│   └── dashboard.png   # Dashboard screenshot
├── data/raw/           # Raw CSV files (gitignored)
└── tests/
    └── test_transformer.py

## Setup
1. pip install -r requirements.txt
2. createdb uber_analytics
3. psql -U postgres -d uber_analytics -f warehouse/schema.sql
4. psql -U postgres -d uber_analytics -f warehouse/views.sql
5. cp .env.example .env  # add your DATABASE_URL
6. python pipeline.py --file 2025-01-15.csv

## Key Results
- 500 trips processed · 100% data quality pass rate
- $27,207 gross revenue tracked
- Zone-level demand analysis
- Driver leaderboard with earnings and ratings
- Surge pricing analysis by hour and zone

## Tech Stack
Python · pandas · SQLAlchemy · psycopg2 · PostgreSQL · Power BI · DAX · Git



## Stack
- Python 3.12 (pandas, SQLAlchemy, psycopg2)
- PostgreSQL 18.4
- Power BI Desktop (DirectQuery)

## Architecture
CSV → Extract → Transform → Load → PostgreSQL → 5 Views → Power BI

## Project Structure
Uber-analytics/
├── pipeline.py         # ETL orchestrator
├── etl/
│   ├── extractor.py    # CSV / API ingestion
│   ├── transformer.py  # Clean & enrich
│   └── loader.py       # PostgreSQL writer
├── warehouse/
│   ├── schema.sql      # Star schema DDL
│   └── views.sql       # 5 analytical views
├── powerbi/
│   └── measures.dax    # 30+ DAX measures
├── data/raw/           # Raw CSV files (gitignored)
└── tests/
    └── test_transformer.py

## Setup
1. pip install -r requirements.txt
2. createdb uber_analytics
3. psql -U postgres -d uber_analytics -f warehouse/schema.sql
4. psql -U postgres -d uber_analytics -f warehouse/views.sql
5. cp .env.example .env  # add your DATABASE_URL
6. python pipeline.py --file 2025-01-15.csv

## Result
- 500 trips loaded
- $27,207 gross revenue
- 100% pass rate
