"""
loader.py — Data Warehouse Load Layer
Writes clean trip data into a PostgreSQL star-schema warehouse.
Supports full-refresh and incremental (upsert) strategies.
"""

import logging
import os
from contextlib import contextmanager
from typing import Literal

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

logger = logging.getLogger(__name__)

LoadStrategy = Literal["replace", "append", "upsert"]


class DataWarehouseLoader:
    """
    Loads transformed trip data into the star schema:

        fact_trips
          ├── dim_drivers
          ├── dim_riders
          ├── dim_zones
          └── dim_time
    """

    FACT_TABLE = "fact_trips"
    DIM_TABLES = ["dim_drivers", "dim_riders", "dim_zones", "dim_time"]
    UPSERT_KEY = "trip_id"

    def __init__(self, db_url: str | None = None):
        """
        db_url: SQLAlchemy connection string.
        Falls back to env var DATABASE_URL.
        Raises clearly if neither is set.
        """
        self.db_url = db_url or os.getenv("DATABASE_URL")

        if not self.db_url:
            raise EnvironmentError(
                "No database URL found.\n"
                "Set DATABASE_URL in your .env file.\n"
                "Example: DATABASE_URL=postgresql+psycopg2://postgres:password@localhost:5432/uber_analytics"
            )

        # Auto-fix: ensure psycopg2 driver prefix
        if self.db_url.startswith("postgresql://"):
            self.db_url = self.db_url.replace(
                "postgresql://", "postgresql+psycopg2://", 1
            )

        logger.info(f"Loader configured -- DB: {self._safe_url()}")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def load(
        self,
        df: pd.DataFrame,
        strategy: LoadStrategy = "upsert",
        chunk_size: int = 5_000,
    ) -> dict:
        """
        Orchestrates the full load sequence:
          1. Ensure tables exist
          2. Upsert dimension tables (FK order)
          3. Load fact table
          4. Return row counts per table
        """
        stats = {}
        with self._connect() as engine:
            self._ensure_schema(engine)

            # Dimensions first (FK constraint order)
            stats["dim_zones"]   = self._load_dim_zones(df, engine)
            stats["dim_drivers"] = self._load_dim_drivers(df, engine)
            stats["dim_riders"]  = self._load_dim_riders(df, engine)
            stats["dim_time"]    = self._load_dim_time(df, engine)

            # Fact table
            fact_df = self._build_fact(df)
            stats["fact_trips"] = self._load_fact_trips(
                fact_df, engine, chunk_size
            )

        logger.info(f"Load complete: {stats}")
        return stats

    def load_flagged(self, flagged_df: pd.DataFrame) -> int:
        """Persist quarantined records to the audit/quarantine table."""
        if flagged_df.empty:
            return 0
        with self._connect() as engine:
            flagged_df.to_sql(
                "quarantine_trips", engine,
                if_exists="append", index=False, chunksize=2_000
            )
        logger.info(f"  quarantine_trips: {len(flagged_df):,} rows written")
        return len(flagged_df)

    # ------------------------------------------------------------------
    # Dimension loaders
    # ------------------------------------------------------------------

    def _load_dim_zones(self, df: pd.DataFrame, engine: Engine) -> int:
        """
        Insert new zone names only — zone_id is SERIAL (auto-increment)
        so we never pass it; PostgreSQL assigns it automatically.
        """
        zones = pd.concat([
            df[["pickup_zone"]].rename(columns={"pickup_zone": "zone_name"}),
            df[["dropoff_zone"]].rename(columns={"dropoff_zone": "zone_name"}),
        ]).drop_duplicates(subset=["zone_name"])

        # Fetch existing zone names to avoid unique-constraint violations
        with engine.connect() as conn:
            try:
                existing = pd.read_sql(
                    "SELECT zone_name FROM dim_zones", conn
                )
                existing_names = set(existing["zone_name"].tolist())
            except Exception:
                existing_names = set()

        new_zones = zones[~zones["zone_name"].isin(existing_names)].copy()

        if new_zones.empty:
            logger.info("  dim_zones: 0 rows written (all zones already exist)")
            return 0

        # Only insert zone_name — let SERIAL handle zone_id
        new_zones[["zone_name"]].to_sql(
            "dim_zones", engine,
            if_exists="append", index=False, chunksize=500
        )
        logger.info(f"  dim_zones: {len(new_zones):,} rows written (upsert)")
        return len(new_zones)

    def _load_dim_drivers(self, df: pd.DataFrame, engine: Engine) -> int:
        """Upsert driver records — delete existing then re-insert."""
        drivers = (
            df[["driver_id", "vehicle_type", "rating_by_rider"]]
            .groupby("driver_id")
            .agg(
                vehicle_type=("vehicle_type", "first"),
                avg_rating=("rating_by_rider", "mean"),
                total_trips=("rating_by_rider", "count"),
            )
            .round({"avg_rating": 2})
            .reset_index()
        )

        driver_ids = drivers["driver_id"].tolist()
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM dim_drivers WHERE driver_id = ANY(:ids)"),
                {"ids": driver_ids}
            )

        drivers.to_sql(
            "dim_drivers", engine,
            if_exists="append", index=False, chunksize=1_000
        )
        logger.info(f"  dim_drivers: {len(drivers):,} rows written (upsert)")
        return len(drivers)

    def _load_dim_riders(self, df: pd.DataFrame, engine: Engine) -> int:
        """Upsert rider records — delete existing then re-insert."""
        riders = (
            df[["rider_id", "payment_method"]]
            .groupby("rider_id")
            .agg(preferred_payment=("payment_method", lambda x: x.mode()[0]))
            .reset_index()
        )

        rider_ids = riders["rider_id"].tolist()
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM dim_riders WHERE rider_id = ANY(:ids)"),
                {"ids": rider_ids}
            )

        riders.to_sql(
            "dim_riders", engine,
            if_exists="append", index=False, chunksize=1_000
        )
        logger.info(f"  dim_riders: {len(riders):,} rows written (upsert)")
        return len(riders)

    def _load_dim_time(self, df: pd.DataFrame, engine: Engine) -> int:
        """Insert new date+hour combinations only."""
        cols = [
            "date", "hour_of_day", "day_of_week", "week_number",
            "month", "year", "is_weekend", "is_peak_hour", "is_late_night"
        ]
        available = [c for c in cols if c in df.columns]
        time_df = (
            df[available]
            .drop_duplicates(subset=["date", "hour_of_day"])
            .copy()
        )

        # Fetch existing date+hour combos
        with engine.connect() as conn:
            try:
                existing = pd.read_sql(
                    "SELECT date, hour_of_day FROM dim_time", conn
                )
                existing["_key"] = (
                    existing["date"].astype(str) + "_"
                    + existing["hour_of_day"].astype(str)
                )
                existing_keys = set(existing["_key"].tolist())
            except Exception:
                existing_keys = set()

        time_df["_key"] = (
            time_df["date"].astype(str) + "_"
            + time_df["hour_of_day"].astype(str)
        )
        new_time = time_df[~time_df["_key"].isin(existing_keys)].copy()
        new_time = new_time.drop(columns=["_key"])

        if new_time.empty:
            logger.info("  dim_time: 0 rows written (all slots already exist)")
            return 0

        new_time.to_sql(
            "dim_time", engine,
            if_exists="append", index=False, chunksize=2_000
        )
        logger.info(f"  dim_time: {len(new_time):,} rows written (upsert)")
        return len(new_time)

    # ------------------------------------------------------------------
    # Fact table builder & loader
    # ------------------------------------------------------------------

    def _build_fact(self, df: pd.DataFrame) -> pd.DataFrame:
        fact_cols = [
            "trip_id", "driver_id", "rider_id",
            "pickup_zone", "dropoff_zone",
            "start_time", "end_time",
            "date", "hour_of_day",
            "fare_amount", "surge_multiplier", "gross_revenue",
            "platform_fee", "driver_payout",
            "trip_distance_km", "duration_minutes", "avg_speed_kmh",
            "vehicle_type", "payment_method",
            "rating_by_rider", "is_peak_hour",
        ]
        missing = [c for c in fact_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Fact build failed -- missing columns: {missing}")
        return df[fact_cols].copy()

    def _load_fact_trips(
        self,
        df: pd.DataFrame,
        engine: Engine,
        chunk_size: int,
    ) -> int:
        """Delete existing trip_ids then insert fresh — PostgreSQL upsert."""
        trip_ids = df["trip_id"].tolist()

        # Remove any existing rows for these trip_ids
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM fact_trips WHERE trip_id = ANY(:ids)"),
                {"ids": trip_ids}
            )

        df.to_sql(
            "fact_trips", engine,
            if_exists="append", index=False, chunksize=chunk_size
        )
        logger.info(f"  fact_trips: {len(df):,} rows written (upsert)")
        return len(df)

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def _ensure_schema(self, engine: Engine) -> None:
        """
        Creates minimal tables if they don't exist.
        For production use, run warehouse/schema.sql via psql instead.
        These are simplified versions without partitioning or FK constraints.
        """
        statements = [
            """
            CREATE TABLE IF NOT EXISTS dim_zones (
                zone_id   SERIAL PRIMARY KEY,
                zone_name TEXT NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_drivers (
                driver_id    TEXT PRIMARY KEY,
                vehicle_type TEXT,
                avg_rating   REAL,
                total_trips  INTEGER
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_riders (
                rider_id          TEXT PRIMARY KEY,
                preferred_payment TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_time (
                date          DATE,
                hour_of_day   INTEGER,
                day_of_week   TEXT,
                week_number   INTEGER,
                month         INTEGER,
                year          INTEGER,
                is_weekend    BOOLEAN,
                is_peak_hour  BOOLEAN,
                is_late_night BOOLEAN,
                PRIMARY KEY (date, hour_of_day)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_trips (
                trip_id          TEXT PRIMARY KEY,
                driver_id        TEXT,
                rider_id         TEXT,
                pickup_zone      TEXT,
                dropoff_zone     TEXT,
                start_time       TIMESTAMP,
                end_time         TIMESTAMP,
                date             DATE,
                hour_of_day      INTEGER,
                fare_amount      REAL,
                surge_multiplier REAL,
                gross_revenue    REAL,
                platform_fee     REAL,
                driver_payout    REAL,
                trip_distance_km REAL,
                duration_minutes REAL,
                avg_speed_kmh    REAL,
                vehicle_type     TEXT,
                payment_method   TEXT,
                rating_by_rider  REAL,
                is_peak_hour     BOOLEAN
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS quarantine_trips (
                trip_id     TEXT,
                flag_reason TEXT,
                raw_data    TEXT
            )
            """,
        ]
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt.strip()))

    def _table_exists(self, table: str, engine: Engine) -> bool:
        """Check table existence using PostgreSQL system catalog."""
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT tablename FROM pg_tables "
                    "WHERE schemaname = 'public' AND tablename = :t"
                ),
                {"t": table}
            )
            return result.fetchone() is not None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _safe_url(self) -> str:
        """Return DB URL with password masked for logging."""
        try:
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(self.db_url)
            if parsed.password:
                masked = parsed._replace(
                    netloc=parsed.netloc.replace(
                        f":{parsed.password}@", ":****@"
                    )
                )
                return urlunparse(masked)
            return self.db_url
        except Exception:
            return "postgresql://****"

    @contextmanager
    def _connect(self):
        engine = create_engine(self.db_url, echo=False)
        try:
            yield engine
        finally:
            engine.dispose()