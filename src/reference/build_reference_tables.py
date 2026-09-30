"""
Builds the three reference (dimension) tables for the pipeline directly
from the raw 311 data: agency, complaint_type, and borough. These are
derived from the actual downloaded data rather than an external source,
so every value referenced by the fact data is guaranteed to have a
matching dimension row.

Reads the raw CSV in chunks (rather than loading all 3.2M+ rows into
memory at once) to keep this runnable on an ordinary laptop.

Usage:
    python src/reference/build_reference_tables.py
    python src/reference/build_reference_tables.py --raw-file data/raw/311_service_requests_2023-01-01_2023-12-31.csv

Output:
    data/reference/dim_agency.csv
    data/reference/dim_complaint_type.csv
    data/reference/dim_borough.csv
    data/reference/reference_build_report.json
"""

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DIR = REPO_ROOT / "data" / "reference"
CHUNK_SIZE = 200_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("reference_tables")

# Custom business-rule categorization layered on top of the source's own
# complaint_type field. This is not provided by NYC Open Data, it's a
# grouping we're defining ourselves for the "workload by category" style
# business questions. Matching is done by keyword, first match wins, in
# the order listed below. Anything that matches nothing falls into Other.
CATEGORY_KEYWORDS = [
    ("Noise", ["noise"]),
    ("Housing & Buildings", [
        "unsanitary condition", "plumbing", "paint/plaster", "door/window", "flooring/stairs",
        "electric", "appliance", "maintenance or facility", "lead", "general", "housing",
        "heat/hot water", "heating", "building", "apartment", "elevator", "construction", "mold",
    ]),
    ("Sanitation", [
        "missed collection", "illegal dumping", "residential disposal", "dirty condition",
        "sanitation", "trash", "litter", "recycl", "graffiti", "sidewalk condition",
    ]),
    ("Parks & Trees", ["tree"]),
    ("Homeless & Social Services", ["homeless", "encampment"]),
    ("Public Safety", [
        "non-emergency police", "drug activity", "illegal fireworks", "safety", "obstruction", "panhandling",
    ]),
    ("Consumer & Business", [
        "consumer complaint", "for hire vehicle", "vendor enforcement", "food establishment", "business complaint",
    ]),
    ("Environmental & Health", ["air quality", "asbestos", "hazardous"]),
    ("Parking & Vehicles", ["parking", "blocked driveway", "derelict vehicle", "abandoned vehicle"]),
    ("Water & Sewer", ["water system", "sewer", "leak", "water conservation"]),
    ("Street & Sidewalk", ["street condition", "pothole", "curb condition", "street light", "traffic signal"]),
    ("Animal", ["animal"]),
    ("Traffic & Safety", ["traffic", "illegal parking", "blocked crosswalk"]),
]


def categorize(complaint_type: str) -> str:
    text = complaint_type.lower()
    for category, keywords in CATEGORY_KEYWORDS:
        if any(keyword in text for keyword in keywords):
            return category
    return "Other"


def parse_args():
    parser = argparse.ArgumentParser(description="Build reference tables from raw 311 data.")
    parser.add_argument(
        "--raw-file",
        default="data/raw/311_service_requests_2023-01-01_2023-12-31.csv",
        help="Path to the raw CSV to build reference tables from.",
    )
    return parser.parse_args()


def build(raw_file: str):
    raw_path = REPO_ROOT / raw_file
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}. Run module 2's acquisition script first.")

    logger.info("Reading %s in chunks of %s", raw_path, CHUNK_SIZE)

    agency_name_counts = defaultdict(Counter)   # agency code -> Counter(agency_name)
    complaint_type_counts = Counter()
    borough_counts = Counter()
    rows_processed = 0

    usecols = ["agency", "agency_name", "complaint_type", "borough"]

    for chunk in pd.read_csv(raw_path, usecols=usecols, dtype=str, chunksize=CHUNK_SIZE):
        chunk = chunk.fillna("UNKNOWN")
        for agency, name in zip(chunk["agency"], chunk["agency_name"]):
            agency_name_counts[agency][name] += 1
        complaint_type_counts.update(chunk["complaint_type"])
        borough_counts.update(chunk["borough"].str.strip().str.upper())
        rows_processed += len(chunk)
        logger.info("Processed %s rows so far", rows_processed)

    # --- dim_agency ---
    agency_conflicts = {}
    agency_rows = []
    for i, (agency_code, name_counter) in enumerate(sorted(agency_name_counts.items()), start=1):
        canonical_name, _ = name_counter.most_common(1)[0]
        if len(name_counter) > 1:
            agency_conflicts[agency_code] = dict(name_counter)
        agency_rows.append({
            "agency_id": i,
            "agency_code": agency_code,
            "agency_name": canonical_name,
            "record_count": sum(name_counter.values()),
        })
    agency_df = pd.DataFrame(agency_rows)
    agency_df.to_csv(REFERENCE_DIR / "dim_agency.csv", index=False)
    logger.info("Wrote dim_agency.csv with %s agencies (%s had name conflicts)", len(agency_df), len(agency_conflicts))

    # --- dim_complaint_type ---
    complaint_rows = []
    other_count = 0
    for i, (complaint_type, count) in enumerate(sorted(complaint_type_counts.items()), start=1):
        category = categorize(complaint_type)
        if category == "Other":
            other_count += count
        complaint_rows.append({
            "complaint_type_id": i,
            "complaint_type": complaint_type,
            "category": category,
            "record_count": count,
        })
    complaint_df = pd.DataFrame(complaint_rows)
    complaint_df.to_csv(REFERENCE_DIR / "dim_complaint_type.csv", index=False)
    other_pct = round(100 * other_count / rows_processed, 2) if rows_processed else 0
    logger.info(
        "Wrote dim_complaint_type.csv with %s complaint types (%s%% of records fell into 'Other')",
        len(complaint_df), other_pct,
    )

    # --- dim_borough ---
    borough_rows = []
    for i, (borough, count) in enumerate(sorted(borough_counts.items()), start=1):
        borough_rows.append({
            "borough_id": i,
            "borough": borough,
            "record_count": count,
        })
    borough_df = pd.DataFrame(borough_rows)
    borough_df.to_csv(REFERENCE_DIR / "dim_borough.csv", index=False)
    logger.info("Wrote dim_borough.csv with %s boroughs", len(borough_df))

    # --- build report ---
    report = {
        "rows_processed": rows_processed,
        "agency_count": len(agency_df),
        "agency_name_conflicts": agency_conflicts,
        "complaint_type_count": len(complaint_df),
        "complaint_type_other_category_pct": other_pct,
        "borough_count": len(borough_df),
        "borough_values": {row["borough"]: row["record_count"] for row in borough_rows},
    }
    report_path = REFERENCE_DIR / "reference_build_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info("Wrote build report to %s", report_path)

    return report


if __name__ == "__main__":
    args = parse_args()
    build(args.raw_file)