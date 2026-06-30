"""
tests/test_transformer.py
Unit tests for UberDataTransformer using synthetic data.
"""

import pandas as pd
import pytest
from faker import Faker
from etl.transformer import UberDataTransformer

fake = Faker()


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

def make_trip_row(**overrides):
    base = {
        "trip_id":          fake.uuid4(),
        "driver_id":        fake.uuid4(),
        "rider_id":         fake.uuid4(),
        "start_time":       "2025-06-15 08:30:00",
        "end_time":         "2025-06-15 08:48:00",
        "pickup_lat":       40.748,
        "pickup_lon":       -73.985,
        "dropoff_lat":      40.710,
        "dropoff_lon":      -74.000,
        "fare_amount":      18.50,
        "surge_multiplier": 1.2,
        "trip_distance_km": 5.2,
        "vehicle_type":     "UberX",
        "payment_method":   "Card",
        "rating_by_rider":  4.8,
    }
    base.update(overrides)
    return base


def make_df(rows=20, **overrides):
    return pd.DataFrame([make_trip_row(**overrides) for _ in range(rows)])


# ------------------------------------------------------------------
# Deduplication
# ------------------------------------------------------------------

class TestDeduplication:
    def test_removes_duplicate_trip_ids(self):
        df = make_df(rows=10)
        df = pd.concat([df, df.iloc[:3]])  # inject 3 dupes
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        assert clean["trip_id"].is_unique

    def test_no_rows_lost_when_unique(self):
        df = make_df(rows=15)
        t = UberDataTransformer()
        clean, flagged = t.transform(df)
        assert len(clean) + len(flagged) == 15


# ------------------------------------------------------------------
# Null handling
# ------------------------------------------------------------------

class TestNullHandling:
    def test_null_fare_goes_to_flagged(self):
        df = make_df(rows=10)
        df.loc[0, "fare_amount"] = None
        t = UberDataTransformer()
        clean, flagged = t.transform(df)
        assert len(flagged) >= 1
        assert len(clean) == 9

    def test_null_driver_id_goes_to_flagged(self):
        df = make_df(rows=5)
        df.loc[2, "driver_id"] = None
        t = UberDataTransformer()
        _, flagged = t.transform(df)
        assert len(flagged) >= 1

    def test_flagged_records_have_flag_reason(self):
        df = make_df(rows=5)
        df.loc[0, "fare_amount"] = None
        t = UberDataTransformer()
        _, flagged = t.transform(df)
        assert "flag_reason" in flagged.columns


# ------------------------------------------------------------------
# Fare clipping
# ------------------------------------------------------------------

class TestFareClipping:
    def test_fare_below_minimum_clipped(self):
        df = make_df(rows=5, fare_amount=0.50)
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        assert (clean["fare_amount"] >= t.FARE_MIN).all()

    def test_fare_above_maximum_clipped(self):
        df = make_df(rows=5, fare_amount=9_999.99)
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        assert (clean["fare_amount"] <= t.FARE_MAX).all()


# ------------------------------------------------------------------
# Derived columns
# ------------------------------------------------------------------

class TestDerivedColumns:
    def test_duration_minutes_positive(self):
        df = make_df(rows=10)
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        assert (clean["duration_minutes"] > 0).all()

    def test_gross_revenue_equals_fare_times_surge(self):
        df = make_df(rows=5, fare_amount=20.0, surge_multiplier=1.5)
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        expected = round(20.0 * 1.5, 2)
        assert (clean["gross_revenue"] == expected).all()

    def test_driver_payout_less_than_gross(self):
        df = make_df(rows=10)
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        assert (clean["driver_payout"] < clean["gross_revenue"]).all()


# ------------------------------------------------------------------
# Zone tagging
# ------------------------------------------------------------------

class TestZoneTagging:
    def test_airport_zone_detected(self):
        df = make_df(rows=3, pickup_lat=40.635, pickup_lon=-73.790)
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        assert (clean["pickup_zone"] == "airport").all()

    def test_unknown_coords_tagged_other(self):
        df = make_df(rows=3, pickup_lat=51.505, pickup_lon=-0.091)  # London
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        assert (clean["pickup_zone"] == "other").all()


# ------------------------------------------------------------------
# Time dimension expansion
# ------------------------------------------------------------------

class TestTimeDimension:
    def test_peak_hour_flag_set(self):
        df = make_df(rows=5, start_time="2025-06-15 08:00:00")  # 8am = peak
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        assert clean["is_peak_hour"].all()

    def test_off_peak_flag_not_set(self):
        df = make_df(rows=5, start_time="2025-06-15 14:00:00")  # 2pm = off-peak
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        assert not clean["is_peak_hour"].any()

    def test_weekend_flag_correct(self):
        df = make_df(rows=5, start_time="2025-06-14 10:00:00")  # Saturday
        t = UberDataTransformer()
        clean, _ = t.transform(df)
        assert clean["is_weekend"].all()


# ------------------------------------------------------------------
# Quality report
# ------------------------------------------------------------------

class TestQualityReport:
    def test_quality_report_generated(self):
        df = make_df(rows=20)
        t = UberDataTransformer()
        t.transform(df)
        assert "raw_rows" in t.quality_report
        assert "pass_rate_pct" in t.quality_report

    def test_pass_rate_100_for_clean_data(self):
        df = make_df(rows=10)
        t = UberDataTransformer()
        t.transform(df)
        assert t.quality_report["pass_rate_pct"] == 100.0