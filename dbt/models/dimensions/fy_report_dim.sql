{{ config(materialized='table') }}

WITH fy_general_data AS (
    SELECT
        report_id AS fy_report_id,
        reg_code,
        fiscal_year,
        period_start,
        period_end,
        submission_date,
        CASE
            WHEN audited = 'Jah' THEN TRUE
            ELSE FALSE
        END AS audited,
        audit_type,
        audit_decision
    FROM {{ ref('staging_report_general_data') }}
),
unique_date_dim AS (
    SELECT
        fiscal_year,
        MIN(date_id) AS date_id
    FROM {{ ref('date_dim') }}
    GROUP BY fiscal_year
)

SELECT
    fy.fy_report_id,
    ed.entity_id,
    udd.date_id AS fiscal_year_id,
    fy.fiscal_year,
    fy.period_start,
    fy.period_end,
    fy.submission_date,
    fy.audited,
    fy.audit_type,
    fy.audit_decision
FROM fy_general_data fy
INNER JOIN {{ ref('entity_dim') }} ed
    ON fy.reg_code = ed.reg_code
LEFT JOIN unique_date_dim udd
    ON fy.fiscal_year = udd.fiscal_year