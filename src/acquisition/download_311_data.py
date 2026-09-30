"""
Downloads a defined subset of the NYC 311 Service Requests dataset from
NYC Open Data (Socrata) and saves it as raw CSV under data/raw/.

Dataset: 311 Service Requests from 2010 to Present
Dataset ID: erm2-nwe9
Portal page: https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9
License: Public domain (NYC Open Data terms of use)

Pagination strategy:
    Uses keyset pagination on a compound (created_date, unique_key)
    cursor rather than $offset. created_date is the fast, indexed field
    and does the heavy lifting; unique_key (stored as Text in this
    dataset, despite the data dictionary calling it BIGINT) only breaks
    ties between rows sharing the exact same timestamp. Sorting or
    filtering on unique_key alone is comparatively slow since it has no
    supporting index, and $offset pagination gets progressively slower
    and eventually times out on large result sets. This compound
    approach keeps each page's cost roughly constant no matter how deep
    into the dataset you are, while staying fast.

Resume behavior:
    If the target output CSV already exists, the script reads the last
    row's created_date and unique_key and resumes from there in append
    mode, instead of starting over. This means an interrupted run can
    simply be re-run with the same command.

Usage:
    python src/acquisition/download_311_data.py
    python src/acquisition/download_311_data.py --start 2023-01-01 --end 2023-12-31
    python src/acquisition/download_311_data.py --max-records 100000   # quick test pull
    python src/acquisition/download_311_data.py --restart              # ignore existing file, start over

Output:
    data/raw/311_service_requests_<start>_<end>.csv
    data/metadata/acquisition_manifest_<start>_<end>.json
"""

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import requests

SOCRATA_BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 25000
MAX_RETRIES = 8
RETRY_BACKOFF_SECONDS = 10
REQUEST_TIMEOUT_SECONDS = 180

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
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Ignore any existing output file and start the download over from scratch.",
    )
    return parser.parse_args()


def build_where_clause(start_date: str, end_date: str, cursor: Optional[Tuple[str, str]]) -> str:
    base = f"(created_date between '{start_date}T00:00:00' and '{end_date}T23:59:59')"
    if cursor is not None:
        cursor_date, cursor_key = cursor
        # unique_key is stored as Text in this dataset despite the data
        # dictionary calling it BIGINT, so it must be quoted as a string.
        # created_date does the heavy lifting since it's an indexed
        # field; unique_key only breaks ties within the same timestamp.
        base += (
            f" AND ((created_date > '{cursor_date}') OR "
            f"(created_date = '{cursor_date}' AND unique_key > '{cursor_key}'))"
        )
    return base


def get_resume_cursor(output_path: Path) -> Tuple[Optional[Tuple[str, str]], int]:
    """
    Reads the last complete row of an existing output CSV to find the
    (created_date, unique_key) cursor to resume from, and counts existing
    rows so the record count stays accurate across a resumed run.
    Returns ((created_date, unique_key), row_count), or (None, 0) if the
    file doesn't exist or has no usable data rows yet.
    """
    if not output_path.exists() or output_path.stat().st_size == 0:
        return None, 0

    with open(output_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if "unique_key" not in fieldnames or "created_date" not in fieldnames:
            logger.warning("Existing file is missing created_date or unique_key, cannot resume from it safely.")
            return None, 0

        last_row = None
        row_count = 0
        for row in reader:
            last_row = row
            row_count += 1

    if last_row is None:
        return None, 0

    created_date = last_row.get("created_date")
    unique_key = last_row.get("unique_key")
    if not created_date or not unique_key:
        logger.warning("Last row is missing created_date or unique_key, cannot resume from it safely.")
        return None, 0

    return (created_date, unique_key), row_count


def fetch_page(session: requests.Session, where_clause: str, limit: int, app_token: Optional[str]):
    params = {
        "$where": where_clause,
        "$order": "created_date, unique_key",
        "$limit": limit,
    }
    headers = {"X-App-Token": app_token} if app_token else {}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                SOCRATA_BASE_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS
            )
            if response.status_code == 200:
                return response.json()

            logger.warning(
                "Request returned status %s on attempt %s/%s. Response body: %s",
                response.status_code, attempt, MAX_RETRIES, response.text[:500],
            )
            if response.status_code == 400:
                return None
        except requests.RequestException as exc:
            logger.warning(
                "Request failed on attempt %s/%s: %s",
                attempt, MAX_RETRIES, exc,
            )
        time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    return None


def download(start_date: str, end_date: str, max_records: Optional[int], app_token: Optional[str], restart: bool) -> dict:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    output_path = RAW_DIR / f"311_service_requests_{start_date}_{end_date}.csv"

    if restart and output_path.exists():
        logger.info("--restart passed, deleting existing file %s", output_path)
        output_path.unlink()

    cursor, total_written = get_resume_cursor(output_path)
    file_mode = "a" if cursor is not None else "w"

    if cursor is not None:
        logger.info(
            "Existing file found with %s records already. Resuming after created_date=%s, unique_key=%s",
            total_written, cursor[0], cursor[1],
        )
    else:
        logger.info("Starting a fresh download for range %s to %s", start_date, end_date)

    logger.info("Output file: %s", output_path)

    session = requests.Session()
    started_at = datetime.utcnow().isoformat()
    fieldnames = None
    failed = False

    with open(output_path, file_mode, newline="", encoding="utf-8") as f:
        writer = None
        if file_mode == "a":
            with open(output_path, "r", newline="", encoding="utf-8") as existing:
                fieldnames = csv.DictReader(existing).fieldnames

        while True:
            page_limit = PAGE_SIZE
            if max_records is not None:
                remaining = max_records - total_written
                if remaining <= 0:
                    break
                page_limit = min(PAGE_SIZE, remaining)

            where_clause = build_where_clause(start_date, end_date, cursor)
            page = fetch_page(session, where_clause, page_limit, app_token)

            if page is None:
                logger.error(
                    "Giving up. %s records are safely saved so far. "
                    "Check the response body logged above, fix if needed, then "
                    "re-run the exact same command to resume from here.",
                    total_written,
                )
                failed = True
                break

            if not page:
                logger.info("No more records returned, download is complete.")
                break

            if fieldnames is None:
                fieldnames = sorted({key for row in page for key in row.keys()})
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
            elif writer is None:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")

            for row in page:
                writer.writerow(row)

            total_written += len(page)
            last_row = page[-1]
            cursor = (last_row["created_date"], last_row["unique_key"])
            f.flush()
            logger.info("Fetched %s records so far (last created_date %s)", total_written, cursor[0])

            if len(page) < page_limit:
                logger.info("Received a partial page, download is complete.")
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
        "last_cursor_created_date": cursor[0] if cursor else None,
        "last_cursor_unique_key": cursor[1] if cursor else None,
        "completed": not failed,
        "output_file": str(output_path.relative_to(REPO_ROOT)),
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
    }

    manifest_path = METADATA_DIR / f"acquisition_manifest_{start_date}_{end_date}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    if failed:
        logger.info("%s records written so far to %s (incomplete)", total_written, output_path)
    else:
        logger.info("Done. %s records written to %s", total_written, output_path)
    logger.info("Manifest written to %s", manifest_path)

    return manifest


if __name__ == "__main__":
    args = parse_args()
    download(args.start, args.end, args.max_records, args.app_token, args.restart)