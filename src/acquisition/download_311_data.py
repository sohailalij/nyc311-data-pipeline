"""
Downloads a defined subset of the NYC 311 Service Requests dataset from
NYC Open Data (Socrata) and saves it as raw CSV under data/raw/.

Dataset: 311 Service Requests from 2010 to Present
Dataset ID: erm2-nwe9
Portal page: https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9
License: Public domain (NYC Open Data terms of use)

This script requires internet access and is meant to be run on your own
machine, not inside a sandboxed environment. No AWS or paid service is
involved. The Socrata API is free and does not require an account, though
a free app token (see README section below) raises the rate limit if you
plan to pull more than a year of data.

Usage:
    python src/acquisition/download_311_data.py
    python src/acquisition/download_311_data.py --start 2023-01-01 --end 2023-12-31
    python src/acquisition/download_311_data.py --max-records 100000   # quick test pull

Output:
    data/raw/311_service_requests_<start>_<end>.csv
    data/metadata/acquisition_manifest_<timestamp>.json
"""

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

SOCRATA_BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 50000
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 5

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw"
METADATA_DIR = REPO_ROOT / "data" / "metadata"
LOG_DIR = REPO_ROOT / "logs"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("acquisition")


def parse_args():
    parser = argparse.ArgumentParser(description="Download NYC 311 data from Socrata.")
    parser.add_argument("--start", default="2023-01-01", help="Start date, inclusive, YYYY-MM-DD.")
    parser.add_argument("--end", default="2023-12-31", help="End date, inclusive, YYYY-MM-DD.")
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Optional cap on total records, useful for a quick test run before pulling the full range.",
    )
    parser.add_argument(
        "--app-token",
        default=None,
        help="Optional free Socrata app token, raises the throttling limit. Not required.",
    )
    return parser.parse_args()


def build_where_clause(start_date: str, end_date: str) -> str:
    return f"created_date between '{start_date}T00:00:00' and '{end_date}T23:59:59'"


def fetch_page(session: requests.Session, where_clause: str, limit: int, offset: int, app_token: str | None):
    params = {
        "$where": where_clause,
        "$order": "created_date",
        "$limit": limit,
        "$offset": offset,
    }
    headers = {"X-App-Token": app_token} if app_token else {}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(SOCRATA_BASE_URL, params=params, headers=headers, timeout=60)
            if response.status_code == 200:
                return response.json()
            logger.warning(
                "Request returned status %s on attempt %s/%s (offset=%s)",
                response.status_code, attempt, MAX_RETRIES, offset,
            )
        except requests.RequestException as exc:
            logger.warning(
                "Request failed on attempt %s/%s (offset=%s): %s",
                attempt, MAX_RETRIES, offset, exc,
            )
        time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError(f"Failed to fetch offset {offset} after {MAX_RETRIES} attempts.")


def download(start_date: str, end_date: str, max_records: int | None, app_token: str | None) -> dict:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    where_clause = build_where_clause(start_date, end_date)
    output_path = RAW_DIR / f"311_service_requests_{start_date}_{end_date}.csv"

    logger.info("Starting download for range %s to %s", start_date, end_date)
    logger.info("Output file: %s", output_path)

    session = requests.Session()
    offset = 0
    total_written = 0
    columns_written = False
    started_at = datetime.utcnow().isoformat()

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = None
        while True:
            remaining = None
            page_limit = PAGE_SIZE
            if max_records is not None:
                remaining = max_records - total_written
                if remaining <= 0:
                    break
                page_limit = min(PAGE_SIZE, remaining)

            page = fetch_page(session, where_clause, page_limit, offset, app_token)

            if not page:
                logger.info("No more records returned, stopping at offset %s", offset)
                break

            if not columns_written:
                fieldnames = sorted({key for row in page for key in row.keys()})
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                columns_written = True

            for row in page:
                writer.writerow(row)

            total_written += len(page)
            offset += len(page)
            logger.info("Fetched %s records so far (offset now %s)", total_written, offset)

            if len(page) < page_limit:
                logger.info("Received a partial page, this is the last page.")
                break

    finished_at = datetime.utcnow().isoformat()

    manifest = {
        "dataset_id": "erm2-nwe9",
        "dataset_name": "311 Service Requests from 2010 to Present",
        "source_url": "https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9",
        "query_start_date": start_date,
        "query_end_date": end_date,
        "max_records_cap": max_records,
        "total_records_downloaded": total_written,
        "output_file": str(output_path.relative_to(REPO_ROOT)),
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
    }

    manifest_path = METADATA_DIR / f"acquisition_manifest_{start_date}_{end_date}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    logger.info("Done. %s records written to %s", total_written, output_path)
    logger.info("Manifest written to %s", manifest_path)

    return manifest


if __name__ == "__main__":
    args = parse_args()
    download(args.start, args.end, args.max_records, args.app_token)
