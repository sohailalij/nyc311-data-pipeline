"""
Ingestion layer: validates the raw 311 CSV against a required schema and
row-level data quality rules, using the reference tables from module 3
for referential integrity checks. Valid rows and rejected rows are
written to separate files, along with a report summarizing what was
found and whether it stayed within the thresholds set in config.yaml.

This stage does not fix or clean anything, it only classifies each row
as valid or invalid and records exactly why, so nothing bad silently
flows downstream. Cleaning and transformation happen in module 5 (Spark).

Usage:
    python src/ingestion/validate_and_ingest.py
    python src/ingestion/validate_and_ingest.py --raw-file data/raw/311_service_requests_2023-01-01_2023-12-31.csv

Output:
    data/processed/valid_311_requests.csv
    data/processed/rejected_311_requests.csv
    data/processed/ingestion_report.json
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utilities.config import load_config  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
REFERENCE_DIR = REPO_ROOT / "data" / "reference"
LOG_DIR = REPO_ROOT / "logs"
CHUNK_SIZE = 200_000

REQUIRED_COLUMNS = [
    "unique_key", "created_date", "closed_date", "agency", "agency_name",
    "complaint_type", "descriptor", "borough", "incident_zip", "status",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ingestion")


def parse_args():
    parser = argparse.ArgumentParser(description="Validate and classify raw 311 data.")
    parser.add_argument(
        "--raw-file",
        default="data/raw/311_service_requests_2023-01-01_2023-12-31.csv",
        help="Path to the raw CSV to validate.",
    )
    return parser.parse_args()


def load_reference_sets():
    agency_df = pd.read_csv(REFERENCE_DIR / "dim_agency.csv", dtype=str)
    complaint_df = pd.read_csv(REFERENCE_DIR / "dim_complaint_type.csv", dtype=str)
    borough_df = pd.read_csv(REFERENCE_DIR / "dim_borough.csv", dtype=str)
    return (
        set(agency_df["agency_code"]),
        set(complaint_df["complaint_type"]),
        set(borough_df["borough"]),
    )


def is_valid_zip(value) -> bool:
    if pd.isna(value):
        return True  # missing zip is its own thing, not a format error
    text = str(value).strip()
    return text.isdigit() and len(text) == 5


def validate_chunk(chunk: pd.DataFrame, valid_agencies, valid_complaint_types, valid_boroughs, seen_keys: set):
    flags = pd.DataFrame(index=chunk.index)

    flags["missing_unique_key"] = chunk["unique_key"].isna() | (chunk["unique_key"].astype(str).str.strip() == "")
    flags["missing_agency"] = chunk["agency"].isna() | (chunk["agency"].astype(str).str.strip() == "")
    flags["missing_complaint_type"] = chunk["complaint_type"].isna() | (chunk["complaint_type"].astype(str).str.strip() == "")

    key_strs = chunk["unique_key"].astype(str)
    flags["duplicate_unique_key"] = key_strs.isin(seen_keys)
    seen_keys.update(key_strs[~flags["duplicate_unique_key"]])

    created = pd.to_datetime(chunk["created_date"], errors="coerce")
    flags["invalid_created_date"] = created.isna()

    closed = pd.to_datetime(chunk["closed_date"], errors="coerce")
    has_closed = chunk["closed_date"].notna() & (chunk["closed_date"].astype(str).str.strip() != "")
    flags["closed_before_created"] = has_closed & closed.notna() & created.notna() & (closed < created)

    flags["unknown_agency"] = chunk["agency"].notna() & ~chunk["agency"].astype(str).isin(valid_agencies)
    flags["unknown_complaint_type"] = chunk["complaint_type"].notna() & ~chunk["complaint_type"].astype(str).isin(valid_complaint_types)
    flags["unknown_borough"] = chunk["borough"].notna() & ~chunk["borough"].astype(str).isin(valid_boroughs)

    flags["invalid_zip_format"] = ~chunk["incident_zip"].apply(is_valid_zip)

    def join_reasons(row):
        return ";".join(col for col in flags.columns if row[col])

    chunk = chunk.copy()
    chunk["_rejection_reasons"] = flags.apply(join_reasons, axis=1)
    return chunk


def run(raw_file: str):
    config = load_config()
    thresholds = config.get("data_quality", {})

    raw_path = REPO_ROOT / raw_file
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}. Run module 2's acquisition script first.")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    valid_agencies, valid_complaint_types, valid_boroughs = load_reference_sets()
    logger.info(
        "Loaded reference sets: %s agencies, %s complaint types, %s boroughs",
        len(valid_agencies), len(valid_complaint_types), len(valid_boroughs),
    )

    header = pd.read_csv(raw_path, nrows=0)
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in header.columns]
    if missing_cols:
        raise ValueError(f"Raw file is missing required columns: {missing_cols}")

    valid_path = PROCESSED_DIR / "valid_311_requests.csv"
    rejected_path = PROCESSED_DIR / "rejected_311_requests.csv"

    seen_keys = set()
    reason_counts = {}
    total_rows = 0
    total_valid = 0
    total_rejected = 0
    wrote_valid_header = False
    wrote_rejected_header = False

    with open(valid_path, "w", newline="", encoding="utf-8") as valid_f, \
         open(rejected_path, "w", newline="", encoding="utf-8") as rejected_f:

        for chunk in pd.read_csv(raw_path, dtype=str, chunksize=CHUNK_SIZE):
            classified = validate_chunk(chunk, valid_agencies, valid_complaint_types, valid_boroughs, seen_keys)

            is_valid = classified["_rejection_reasons"] == ""
            valid_rows = classified.loc[is_valid].drop(columns=["_rejection_reasons"])
            rejected_rows = classified.loc[~is_valid]

            valid_rows.to_csv(valid_f, index=False, header=not wrote_valid_header)
            wrote_valid_header = True

            if len(rejected_rows) > 0:
                rejected_rows.to_csv(rejected_f, index=False, header=not wrote_rejected_header)
                wrote_rejected_header = True

            for reasons_str in classified.loc[~is_valid, "_rejection_reasons"]:
                for reason in reasons_str.split(";"):
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1

            total_rows += len(chunk)
            total_valid += len(valid_rows)
            total_rejected += len(rejected_rows)
            logger.info("Processed %s rows so far (%s valid, %s rejected)", total_rows, total_valid, total_rejected)

    pass_rate = round(100 * total_valid / total_rows, 4) if total_rows else 0

    report = {
        "total_rows": total_rows,
        "valid_rows": total_valid,
        "rejected_rows": total_rejected,
        "pass_rate_pct": pass_rate,
        "rejection_reason_counts": reason_counts,
        "threshold_checks": {},
    }

    if total_rows:
        missing_id_rate = reason_counts.get("missing_unique_key", 0) / total_rows
        invalid_date_rate = reason_counts.get("invalid_created_date", 0) / total_rows
        duplicate_rate = reason_counts.get("duplicate_unique_key", 0) / total_rows

        report["threshold_checks"] = {
            "missing_id_rate": {
                "value": round(missing_id_rate, 6),
                "threshold": thresholds.get("max_missing_id_rate"),
                "within_threshold": missing_id_rate <= thresholds.get("max_missing_id_rate", 1),
            },
            "invalid_date_rate": {
                "value": round(invalid_date_rate, 6),
                "threshold": thresholds.get("max_invalid_date_rate"),
                "within_threshold": invalid_date_rate <= thresholds.get("max_invalid_date_rate", 1),
            },
            "duplicate_rate": {
                "value": round(duplicate_rate, 6),
                "threshold": thresholds.get("max_duplicate_rate"),
                "within_threshold": duplicate_rate <= thresholds.get("max_duplicate_rate", 1),
            },
        }

    report_path = PROCESSED_DIR / "ingestion_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    logger.info("Done. %s valid, %s rejected, pass rate %.4f%%", total_valid, total_rejected, pass_rate)
    logger.info("Report written to %s", report_path)

    for name, check in report["threshold_checks"].items():
        status = "OK" if check["within_threshold"] else "EXCEEDED"
        logger.info("Threshold check [%s]: %s (value=%s, threshold=%s)", name, status, check["value"], check["threshold"])

    return report


if __name__ == "__main__":
    args = parse_args()
    run(args.raw_file)