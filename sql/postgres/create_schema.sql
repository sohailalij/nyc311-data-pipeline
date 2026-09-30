-- Monthly summary table
CREATE TABLE IF NOT EXISTS borough_category_monthly_summary (
    id                     SERIAL PRIMARY KEY,
    created_year           INT NOT NULL,
    created_month          INT NOT NULL CHECK (created_month BETWEEN 1 AND 12),
    borough                VARCHAR(20) NOT NULL,
    complaint_category     VARCHAR(50) NOT NULL,
    total_requests         INT NOT NULL CHECK (total_requests >= 0),
    avg_resolution_hours   NUMERIC(10,2),
    closed_requests        INT NOT NULL CHECK (closed_requests >= 0),
    unresolved_requests    INT NOT NULL CHECK (unresolved_requests >= 0),
    UNIQUE (created_year, created_month, borough, complaint_category)
);

CREATE INDEX IF NOT EXISTS idx_summary_year_month ON borough_category_monthly_summary (created_year, created_month);
CREATE INDEX IF NOT EXISTS idx_summary_borough ON borough_category_monthly_summary (borough);

-- Daily summary table (finer granularity)
CREATE TABLE IF NOT EXISTS borough_category_daily_summary (
    id                     SERIAL PRIMARY KEY,
    request_date           DATE NOT NULL,
    borough                VARCHAR(20) NOT NULL,
    complaint_category     VARCHAR(50) NOT NULL,
    total_requests         INT NOT NULL CHECK (total_requests >= 0),
    avg_resolution_hours   NUMERIC(10,2),
    closed_requests        INT NOT NULL CHECK (closed_requests >= 0),
    unresolved_requests    INT NOT NULL CHECK (unresolved_requests >= 0),
    UNIQUE (request_date, borough, complaint_category)
);

CREATE INDEX IF NOT EXISTS idx_daily_date ON borough_category_daily_summary (request_date);
CREATE INDEX IF NOT EXISTS idx_daily_borough ON borough_category_daily_summary (borough);

-- Business-facing view
CREATE OR REPLACE VIEW category_totals_ytd AS
SELECT
    complaint_category,
    SUM(total_requests) AS total_requests,
    ROUND(AVG(avg_resolution_hours), 2) AS avg_resolution_hours
FROM borough_category_monthly_summary
GROUP BY complaint_category
ORDER BY total_requests DESC;