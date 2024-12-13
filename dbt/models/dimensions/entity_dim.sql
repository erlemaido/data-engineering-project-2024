{{ config(materialized='table') }}

WITH latest_general_data AS (
    SELECT
        reg_code,
        CAST(legal_form AS STRING) AS legal_form,
        CAST(status AS STRING) AS status,
        submission_date,
        ROW_NUMBER() OVER (PARTITION BY reg_code ORDER BY submission_date DESC) AS row_num
    FROM {{ ref('staging_report_general_data') }}
),
filtered_general_data AS (
    SELECT
        reg_code,
        legal_form,
        status
    FROM latest_general_data
    WHERE row_num = 1
),

latest_tax_data AS (
    SELECT
        reg_code,
        CAST(entity_name AS STRING) AS entity_name,
        CAST(entity_type AS STRING) AS entity_type,
        CAST(EMTAK_field AS STRING) AS emtak,
        year,
        quarter,
        ROW_NUMBER() OVER (PARTITION BY reg_code ORDER BY year DESC, quarter DESC) AS row_num
    FROM {{ ref('staging_tax_data') }}
),
filtered_tax_data AS (
    SELECT
        reg_code,
        entity_name,
        entity_type,
        emtak
    FROM latest_tax_data
    WHERE row_num = 1
),

combined_data AS (
    SELECT
        COALESCE(gd.reg_code, td.reg_code) AS reg_code,
        gd.legal_form,
        gd.status,
        td.entity_name AS name,
        td.entity_type AS type,
        td.emtak
    FROM filtered_general_data gd
    FULL OUTER JOIN filtered_tax_data td
    ON gd.reg_code = td.reg_code
)

SELECT
    ROW_NUMBER() OVER () AS entity_id,
    cd.*
FROM combined_data cd
