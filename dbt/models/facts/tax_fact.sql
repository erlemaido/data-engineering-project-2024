{{ config(materialized='table') }}

WITH tax_data AS (
    SELECT
        reg_code AS reg_code,
        entity_name AS name,
        entity_type AS type,
        sales_tax_reg AS sales_tax_reg,
        state_taxes AS state_taxes,
        employee_taxes AS labour_taxes,
        revenue AS revenue,
        CAST(employees AS INTEGER) AS nr_employees,
        year AS year,
        quarter AS quarter
    FROM {{ ref('staging_tax_data') }}
)

SELECT
    ROW_NUMBER() OVER () AS tax_data_id,
    ed.entity_id,
    dd.date_id,
    td.year,
    td.quarter,
    td.state_taxes,
    td.labour_taxes,
    td.revenue,
    td.nr_employees,
FROM tax_data td
INNER JOIN {{ ref('entity_dim') }} ed
    ON td.reg_code = ed.reg_code
LEFT JOIN {{ ref('date_dim') }} dd
    ON dd.year = td.year AND dd.quarter = td.quarter