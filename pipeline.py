"""
pipeline.py — Uber Analytics ETL Orchestrator
Wires Extract -> Transform -> Load and emits a quality report.

Usage:
    python pipeline.py --from 2025-01-01 --to 2025-01-31
    python pipeline.py --file 2025-01-15.csv
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from etl.extractor   import UberDataExtractor
from etl.transformer import UberDataTransformer
from etl.loader      import DataWarehouseLoader

# ------------------------------------------------------------------
# Logging setup — Windows-safe (no unicode arrows, utf-8 file handler)
# ------------------------------------------------------------------

Path("logs").mkdir(exist_ok=True)
Path("reports").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-7s] %(name)s -- %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/pipeline.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("pipeline")


# ------------------------------------------------------------------
# Orchestrator
# ------------------------------------------------------------------

class UberETLPipeline:
    """
    End-to-end pipeline:
      1. Extract   -- load raw CSV(s) from data/raw/
      2. Transform -- clean, enrich, zone-tag
      3. Load      -- write to PostgreSQL data warehouse
      4. Report    -- emit quality stats as JSON to reports/
    """

    def __init__(
        self,
        source_dir: str = "data/raw",
        db_url: str | None = None,
        report_dir: str = "reports",
    ):
        self.extractor   = UberDataExtractor(source_dir=source_dir)
        self.transformer = UberDataTransformer()
        self.loader      = DataWarehouseLoader(db_url=db_url)
        self.report_dir  = Path(report_dir)
        self.report_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Batch run — date range
    # ------------------------------------------------------------------

    def run_batch(self, date_from: str, date_to: str) -> dict:
        """
        Process all CSV files between date_from and date_to (inclusive).
        Files must be named YYYY-MM-DD.csv inside data/raw/.
        """
        logger.info(f"=== PIPELINE START  {date_from} >> {date_to} ===")
        t0 = time.perf_counter()

        # Stage 1 -- Extract
        logger.info("[ 1/3 ] EXTRACT")
        raw_df = self.extractor.extract_batch(date_from, date_to)
        logger.info(f"        {len(raw_df):,} rows extracted")

        # Stage 2 -- Transform
        logger.info("[ 2/3 ] TRANSFORM")
        clean_df, flagged_df = self.transformer.transform(raw_df)
        qr = self.transformer.quality_report
        logger.info(
            f"        {qr['clean_rows']:,} clean  |  "
            f"{qr['flagged_rows']:,} flagged  |  "
            f"{qr['pass_rate_pct']}% pass rate"
        )

        # Stage 3 -- Load
        logger.info("[ 3/3 ] LOAD")
        load_stats    = self.loader.load(clean_df, strategy="upsert")
        flagged_count = self.loader.load_flagged(flagged_df)

        elapsed = round(time.perf_counter() - t0, 2)

        report = {
            "run_at":      _now_iso(),
            "mode":        "batch",
            "date_range":  {"from": date_from, "to": date_to},
            "elapsed_sec": elapsed,
            "extract":     {"rows_extracted": self.extractor.total_extracted},
            "transform":   self.transformer.quality_report,
            "load":        load_stats,
            "quarantine":  {"flagged_rows": flagged_count},
        }

        self._save_report(report)
        logger.info(f"=== PIPELINE COMPLETE  {elapsed}s ===")
        return report

    # ------------------------------------------------------------------
    # Single file run
    # ------------------------------------------------------------------

    def run_single_file(self, filename: str) -> dict:
        """
        Process a single CSV file.
        filename can be just the name (2025-01-15.csv) or a full path.
        """
        logger.info(f"=== PIPELINE START  file={filename} ===")
        t0 = time.perf_counter()

        # Stage 1 -- Extract
        logger.info("[ 1/3 ] EXTRACT")
        raw_df = self.extractor.extract_from_csv(filename)
        logger.info(f"        {len(raw_df):,} rows extracted")

        # Stage 2 -- Transform
        logger.info("[ 2/3 ] TRANSFORM")
        clean_df, flagged_df = self.transformer.transform(raw_df)
        qr = self.transformer.quality_report
        logger.info(
            f"        {qr['clean_rows']:,} clean  |  "
            f"{qr['flagged_rows']:,} flagged  |  "
            f"{qr['pass_rate_pct']}% pass rate"
        )

        # Stage 3 -- Load
        logger.info("[ 3/3 ] LOAD")
        load_stats    = self.loader.load(clean_df, strategy="upsert")
        flagged_count = self.loader.load_flagged(flagged_df)

        elapsed = round(time.perf_counter() - t0, 2)

        report = {
            "run_at":      _now_iso(),
            "mode":        "single_file",
            "source_file": filename,
            "elapsed_sec": elapsed,
            "extract":     {"rows_extracted": len(raw_df)},
            "transform":   self.transformer.quality_report,
            "load":        load_stats,
            "quarantine":  {"flagged_rows": flagged_count},
        }

        self._save_report(report)
        logger.info(f"=== PIPELINE COMPLETE  {elapsed}s ===")
        return report

    # ------------------------------------------------------------------
    # Report saver
    # ------------------------------------------------------------------

    def _save_report(self, report: dict) -> None:
        ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = self.report_dir / f"quality_report_{ts}.json"
        path.write_text(
            json.dumps(report, indent=2, default=str),
            encoding="utf-8"
        )
        logger.info(f"Quality report saved >> {path}")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _now_iso() -> str:
    """Return current UTC time as ISO-8601 string (no deprecation warning)."""
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Uber Analytics ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py --file 2025-01-15.csv
  python pipeline.py --from 2025-01-01 --to 2025-01-31
  python pipeline.py --file 2025-01-15.csv --db postgresql+psycopg2://user:pass@host/db
        """
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--from", dest="date_from", metavar="YYYY-MM-DD",
        help="Start date for batch run (requires --to)"
    )
    group.add_argument(
        "--file", dest="filename",
        help="Single CSV filename inside data/raw/ (e.g. 2025-01-15.csv)"
    )

    parser.add_argument(
        "--to", dest="date_to", metavar="YYYY-MM-DD",
        help="End date for batch run (required with --from)"
    )
    parser.add_argument(
        "--db", dest="db_url", default=None,
        help="SQLAlchemy DB URL — overrides DATABASE_URL in .env"
    )
    parser.add_argument(
        "--source-dir", dest="source_dir", default="data/raw",
        help="Directory containing raw CSV files (default: data/raw)"
    )

    args = parser.parse_args()

    pipeline = UberETLPipeline(
        source_dir=args.source_dir,
        db_url=args.db_url,
    )

    if args.filename:
        report = pipeline.run_single_file(args.filename)
    else:
        if not args.date_to:
            parser.error("--to is required when using --from")
        report = pipeline.run_batch(args.date_from, args.date_to)

    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()