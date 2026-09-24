# NYC 311 Service Request Data Pipeline

A local-first, cloud-migrated data engineering and analytics pipeline built on
PySpark, Apache Hive, PostgreSQL, AWS S3, Jenkins, and Tableau. It ingests raw
NYC 311 service request data, validates and cleans it, organizes it into a
Hive warehouse, curates it into a relational analytical layer, and surfaces
it through a Tableau dashboard.

The pipeline runs entirely on free-tier or local resources. No component in
this project requires a paid AWS service.

## Status

This project is under active development. Modules are being built and
checked off in order.

- [x] 1. Project setup
- [ ] 2. Data acquisition (script and docs ready, pending your first local run)
- [ ] 3. Reference tables
- [ ] 4. Ingestion layer
- [ ] 5. Spark cleaning and transformation
- [ ] 6. Spark aggregation
- [ ] 7. Hive warehouse
- [ ] 8. SQL analytics
- [ ] 9. Data quality framework
- [ ] 10. PostgreSQL layer
- [ ] 11. Tableau dashboard
- [ ] 12. Automated tests
- [ ] 13. Jenkins CI/CD
- [ ] 14. AWS migration
- [ ] 15. Documentation

## Repository structure

```text
nyc311-data-pipeline/
├── data/
│   ├── raw/            raw source files (git-ignored, not committed)
│   ├── sample/          small sample files safe to commit for reproducibility
│   └── metadata/        acquisition run manifests (safe to commit, no raw data in them)
├── src/
│   ├── acquisition/       download script for the NYC 311 source data
│   ├── ingestion/        schema validation, ingestion logging
│   ├── spark/            cleaning, transformation, aggregation
│   ├── hive/              Hive table DDL
│   ├── validation/       data quality framework
│   └── utilities/        shared helpers (config loading, logging setup)
├── sql/                   standalone analytical SQL scripts
├── tests/                 pytest suite
├── jenkins/               Jenkinsfile
├── dashboards/tableau/    Tableau workbook(s)
├── docs/                  architecture, end-user, and operations docs
├── config/                config.example.yaml (real config.yaml is git-ignored)
├── requirements.txt
├── .gitignore
└── README.md
```

## Setup

1. Clone the repository.
2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. Copy the config template and fill in local values:
   ```bash
   cp config/config.example.yaml config/config.yaml
   ```
4. Download the raw data (see `docs/Data_Acquisition.md` for full details):
   ```bash
   python src/acquisition/download_311_data.py --start 2023-01-01 --end 2023-12-31
   ```
5. Further setup instructions (Spark, Hive, PostgreSQL, Jenkins, AWS) will be
   added here as each module is completed.

## Cost

This project is built entirely within free tiers. AWS usage is limited to a
small S3 footprint, backed by an AWS Budget alert set at $0.01 so any
unexpected charge is caught immediately. Details are in
`docs/Cloud_Migration.md` once that stage is complete.

## License

Add a license of your choice before making the repository public.
