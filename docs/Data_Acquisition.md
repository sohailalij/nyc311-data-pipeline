# Data Acquisition

## Source

- **Dataset:** 311 Service Requests from 2010 to Present
- **Dataset ID:** `erm2-nwe9`
- **Publisher:** NYC Open Data (data.cityofnewyork.us)
- **Portal page:** https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9
- **API endpoint used:** `https://data.cityofnewyork.us/resource/erm2-nwe9.json` (Socrata Open Data API)
- **License:** Public domain, published under NYC Open Data's terms of use. No cost, no API key required for the volume this project pulls.

## Why this subset

The full dataset goes back to 2010 and holds tens of millions of rows,
far more than needed to demonstrate the pipeline. This project pulls
**one full calendar year, January 1 to December 31, 2023**, which:

- Is large enough to justify Spark's distributed processing (roughly
  2 to 3 million records for a full year)
- Is small enough to download, store, and process on a local machine
  for free
- Gives a full annual cycle for the seasonal/monthly trend questions in
  the analytics stage
- Is recent enough that agency names and complaint type categories are
  current, rather than mixed with older, since-retired category
  spellings from a decade ago

## How to reproduce the download

1. Make sure dependencies are installed:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the acquisition script from the repo root:
   ```bash
   python src/acquisition/download_311_data.py --start 2023-01-01 --end 2023-12-31
   ```
3. For a quick test run before committing to the full year (recommended
   the first time, so you're not waiting on a multi-million-row pull
   just to check the script works):
   ```bash
   python src/acquisition/download_311_data.py --start 2023-01-01 --end 2023-01-07 --max-records 5000
   ```

## Output

- Raw CSV: `data/raw/311_service_requests_<start>_<end>.csv`
- Manifest: `data/metadata/acquisition_manifest_<start>_<end>.json`, recording
  the exact query parameters, record count, and download timestamps for
  every run. Keep these manifests, they're the evidence that the raw data
  is reproducible and unmodified from source.

## Rate limits and the optional app token

Socrata allows anonymous requests without any account, which is what this
script uses by default. Anonymous requests are throttled more aggressively
than authenticated ones. If a full-year pull gets throttled repeatedly:

1. Create a free Socrata account at https://data.cityofnewyork.us/profile/edit
2. Generate an app token under your profile's "Developer Settings"
3. Pass it to the script:
   ```bash
   python src/acquisition/download_311_data.py --app-token YOUR_TOKEN_HERE
   ```

This token is free with no usage charges, it only raises the anonymous
rate limit.

## Re-running for a different window

To pull a different date range later (for example, to extend the analysis
to two years), just re-run the script with different `--start`/`--end`
values. Each run produces its own CSV and manifest rather than overwriting
a prior pull, so multiple windows can coexist under `data/raw/`.
