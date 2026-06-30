"""
extractor.py — Uber Trip Data Extraction Layer
Pulls raw trip data from CSV/API sources into staging area.
"""

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class UberDataExtractor:
    """
    Handles extraction of raw Uber trip data from:
    - Local CSV files (batch mode)
    - Uber Movement API (scheduled ingestion)
    - S3 raw bucket (cloud pipeline)
    """

    REQUIRED_COLUMNS = [
        "trip_id", "driver_id", "rider_id", "start_time", "end_time",
        "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon",
        "fare_amount", "surge_multiplier", "trip_distance_km",
        "vehicle_type", "payment_method", "rating_by_rider",
    ]

    def __init__(self, source_dir: str = "data/raw", api_key: Optional[str] = None):
        self.source_dir = Path(source_dir)
        self.api_key = api_key or os.getenv("UBER_API_KEY")
        self._extracted_count = 0

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def extract_from_csv(self, filename: str) -> pd.DataFrame:
        """Load a single CSV file with basic schema validation."""
        path = self.source_dir / filename
        logger.info(f"Extracting from CSV: {path}")

        df = pd.read_csv(path, parse_dates=["start_time", "end_time"], low_memory=False)
        df = self._validate_schema(df)
        self._extracted_count += len(df)
        logger.info(f"  >> {len(df):,} rows loaded")
        return df

    def extract_batch(self, date_from: str, date_to: str) -> pd.DataFrame:
        """
        Load all CSV files whose names fall in the date range
        (expects files named YYYY-MM-DD.csv in source_dir).
        """
        start = datetime.strptime(date_from, "%Y-%m-%d")
        end   = datetime.strptime(date_to,   "%Y-%m-%d")
        frames = []

        for n in range((end - start).days + 1):
            fname = (start + timedelta(days=n)).strftime("%Y-%m-%d") + ".csv"
            fpath = self.source_dir / fname
            if fpath.exists():
                frames.append(self.extract_from_csv(fname))
            else:
                logger.warning(f"Missing file: {fname}")

        if not frames:
            raise FileNotFoundError(f"No CSV files found between {date_from} and {date_to}")

        combined = pd.concat(frames, ignore_index=True)
        logger.info(f"Batch extract complete: {len(combined):,} total rows")
        return combined

    def extract_from_api(self, endpoint: str, params: dict) -> pd.DataFrame:
        """Pull data from a REST endpoint (e.g., Uber Movement API)."""
        if not self.api_key:
            raise EnvironmentError("UBER_API_KEY not set — cannot call API")

        headers = {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}
        resp = requests.get(endpoint, headers=headers, params=params, timeout=30)
        resp.raise_for_status()

        df = pd.DataFrame(resp.json().get("data", []))
        df = self._validate_schema(df)
        self._extracted_count += len(df)
        return df

    @property
    def total_extracted(self) -> int:
        return self._extracted_count

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Schema mismatch — missing columns: {missing}")
        return df[self.REQUIRED_COLUMNS]  # enforce column order