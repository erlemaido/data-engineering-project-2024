{{ config(materialized='table') }}

WITH date_range AS (
    SELECT
        DATE '2000-01-01' + (i || ' days')::interval AS full_date
    FROM generate_series(0, 36525) AS t(i) -- Generate numbers from 0 to 36525
),

formatted_dates AS (
    SELECT
        full_date,
        EXTRACT(YEAR FROM full_date) AS fiscal_year,
        EXTRACT(YEAR FROM full_date) AS year,
        CEIL(EXTRACT(MONTH FROM full_date) / 3) AS quarter, -- Quarter as a number
        EXTRACT(MONTH FROM full_date) AS month,
        EXTRACT(WEEK FROM full_date) AS week,
        EXTRACT(DAY FROM full_date) AS day,
        CASE
            WHEN EXTRACT(ISODOW FROM full_date) IN (6, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM date_range
)

SELECT
    ROW_NUMBER() OVER () AS date_id,
    full_date,
    fiscal_year,
    year,
    quarter,
    month,
    week,
    day,
    is_weekend
FROM formatted_dates