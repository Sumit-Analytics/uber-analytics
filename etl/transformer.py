"""
transformer.py — Uber Trip Data Transformation Layer
Cleans, enriches, and shapes raw trip data for the data warehouse.
"""

import logging
from typing import Tuple

import numpy as np
import pandas as pd
from geopy.distance import geodesic

logger = logging.getLogger(__name__)


class UberDataTransformer:
    """
    Applies a deterministic sequence of cleaning and enrichment steps.

    Pipeline order:
      1. Drop exact duplicates
      2. Standardise column types
      3. Remove / flag null records
      4. Clip fare & distance outliers
      5. Derive computed columns (duration, speed, revenue)
      6. Geo-zone tagging
      7. Time dimension expansion (hour, day, peak flag)
      8. Validate final record integrity
    """

    # Fare outlier bounds (USD)
    FARE_MIN = 2.50
    FARE_MAX = 500.0

    # Distance outlier bounds (km)
    DIST_MIN = 0.3
    DIST_MAX = 120.0

    # Speed sanity bounds (km/h) — for flagging only, not removal
    SPEED_MIN = 2.0
    SPEED_MAX = 130.0

    def __init__(self, zone_lookup: dict | None = None):
        """
        zone_lookup: {zone_name: (lat_min, lat_max, lon_min, lon_max)}
        If None, a built-in NYC demo set is used.
        """
        self.zone_lookup = zone_lookup or _default_zone_lookup()
        self.quality_report: dict = {}

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def transform(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Returns:
            clean_df    — records that passed all checks
            flagged_df  — records quarantined for review
        """
        raw_count = len(df)
        logger.info(f"Transformer starting — {raw_count:,} input rows")

        df = self._deduplicate(df)
        df = self._coerce_types(df)
        df, flagged = self._split_nulls(df)
        df = self._clip_outliers(df)
        df = self._derive_columns(df)
        df = self._tag_zones(df)
        df = self._expand_time_dims(df)
        df = self._validate_final(df)

        self.quality_report = {
            "raw_rows":      raw_count,
            "clean_rows":    len(df),
            "flagged_rows":  len(flagged),
            "pass_rate_pct": round(len(df) / raw_count * 100, 2),
        }
        logger.info(f"Transform complete — {len(df):,} clean, {len(flagged):,} flagged")
        return df, flagged

    # ------------------------------------------------------------------
    # Step 1 — Deduplication
    # ------------------------------------------------------------------

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(subset=["trip_id"])
        dropped = before - len(df)
        if dropped:
            logger.warning(f"  Dropped {dropped:,} duplicate trip_ids")
        return df

    # ------------------------------------------------------------------
    # Step 2 — Type coercion
    # ------------------------------------------------------------------

    def _coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True, errors="coerce")
        df["end_time"]   = pd.to_datetime(df["end_time"],   utc=True, errors="coerce")
        df["fare_amount"]        = pd.to_numeric(df["fare_amount"],        errors="coerce")
        df["surge_multiplier"]   = pd.to_numeric(df["surge_multiplier"],   errors="coerce").clip(lower=1.0)
        df["trip_distance_km"]   = pd.to_numeric(df["trip_distance_km"],   errors="coerce")
        df["rating_by_rider"]    = pd.to_numeric(df["rating_by_rider"],    errors="coerce").clip(1, 5)
        df["vehicle_type"]       = df["vehicle_type"].str.strip().str.lower()
        df["payment_method"]     = df["payment_method"].str.strip().str.lower()
        return df

    # ------------------------------------------------------------------
    # Step 3 — Null handling
    # ------------------------------------------------------------------

    def _split_nulls(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        critical = ["trip_id", "driver_id", "start_time", "end_time",
                    "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon",
                    "fare_amount"]
        null_mask = df[critical].isnull().any(axis=1)
        flagged = df[null_mask].copy()
        flagged["flag_reason"] = "missing_critical_field"
        clean = df[~null_mask].copy()
        logger.info(f"  Nulls flagged: {len(flagged):,}")
        return clean, flagged

    # ------------------------------------------------------------------
    # Step 4 — Outlier clipping
    # ------------------------------------------------------------------

    def _clip_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        df["fare_amount"]      = df["fare_amount"].clip(self.FARE_MIN, self.FARE_MAX)
        df["trip_distance_km"] = df["trip_distance_km"].clip(self.DIST_MIN, self.DIST_MAX)
        return df

    # ------------------------------------------------------------------
    # Step 5 — Derived columns
    # ------------------------------------------------------------------

    def _derive_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df["duration_minutes"] = (
            (df["end_time"] - df["start_time"]).dt.total_seconds() / 60
        ).clip(lower=1).round(1)

        df["avg_speed_kmh"] = (
            df["trip_distance_km"] / (df["duration_minutes"] / 60)
        ).round(1)

        df["speed_flag"] = (
            (df["avg_speed_kmh"] < self.SPEED_MIN) |
            (df["avg_speed_kmh"] > self.SPEED_MAX)
        )

        # Revenue = base fare × surge
        df["gross_revenue"] = (df["fare_amount"] * df["surge_multiplier"]).round(2)

        # Platform fee (20% of base)
        df["platform_fee"] = (df["fare_amount"] * 0.20).round(2)
        df["driver_payout"] = (df["gross_revenue"] - df["platform_fee"]).round(2)

        return df

    # ------------------------------------------------------------------
    # Step 6 — Zone tagging
    # ------------------------------------------------------------------

    def _tag_zones(self, df: pd.DataFrame) -> pd.DataFrame:
        def lookup_zone(lat, lon):
            for zone, (lat_min, lat_max, lon_min, lon_max) in self.zone_lookup.items():
                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    return zone
            return "other"

        df["pickup_zone"]  = df.apply(lambda r: lookup_zone(r.pickup_lat,  r.pickup_lon),  axis=1)
        df["dropoff_zone"] = df.apply(lambda r: lookup_zone(r.dropoff_lat, r.dropoff_lon), axis=1)
        return df

    # ------------------------------------------------------------------
    # Step 7 — Time dimension expansion
    # ------------------------------------------------------------------

    def _expand_time_dims(self, df: pd.DataFrame) -> pd.DataFrame:
        st = df["start_time"].dt
        df["hour_of_day"]   = st.hour
        df["day_of_week"]   = st.day_name()
        df["date"]          = st.date
        df["week_number"]   = st.isocalendar().week.astype(int)
        df["month"]         = st.month
        df["year"]          = st.year
        df["is_weekend"]    = st.dayofweek >= 5
        df["is_peak_hour"]  = df["hour_of_day"].isin(range(7, 10)) | df["hour_of_day"].isin(range(17, 20))
        df["is_late_night"] = df["hour_of_day"].isin([22, 23, 0, 1, 2, 3])
        return df

    # ------------------------------------------------------------------
    # Step 8 — Final integrity check
    # ------------------------------------------------------------------

    def _validate_final(self, df: pd.DataFrame) -> pd.DataFrame:
        assert df["trip_id"].is_unique, "Duplicate trip_ids survived dedup"
        assert (df["fare_amount"] >= self.FARE_MIN).all(), "Fare below minimum"
        assert (df["duration_minutes"] >= 1).all(),       "Negative durations"
        return df


# ------------------------------------------------------------------
# Helper — default NYC-style zone bounding boxes
# ------------------------------------------------------------------

def _default_zone_lookup() -> dict:
    return {
        "airport":       (40.620, 40.660,  -73.810, -73.770),
        "downtown":      (40.700, 40.720,  -74.020, -73.990),
        "midtown":       (40.748, 40.762,  -73.990, -73.970),
        "upper_east":    (40.762, 40.785,  -73.970, -73.945),
        "upper_west":    (40.762, 40.800,  -73.990, -73.970),
        "brooklyn":      (40.640, 40.710,  -74.030, -73.920),
        "queens":        (40.700, 40.780,  -73.920, -73.820),
        "bronx":         (40.800, 40.880,  -73.940, -73.820),
        "hoboken":       (40.740, 40.760,  -74.040, -74.020),
        "jfk_airport":   (40.630, 40.645,  -73.800, -73.770),
    }