{{ config(materialized='table') }}

WITH entity_data AS (
    SELECT DISTINCT
        reg_code,
        CAST(legal_form AS STRING) AS legal_form,
        CAST(status AS STRING) AS status,
        CAST(NULL AS STRING) AS name,
        CAST(NULL AS STRING) AS type,
        CAST(NULL AS STRING) AS emtak
    FROM {{ ref('staging_report_general_data') }}

    UNION ALL

    SELECT DISTINCT
        reg_code,
        CAST(NULL AS STRING) AS legal_form,
        CAST(NULL AS STRING) AS status,
        CAST(entity_name AS STRING) AS name,
        CAST(entity_type AS STRING) AS type,
        CAST(EMTAK_field AS STRING) AS emtak
    FROM {{ ref('staging_tax_data') }}
),

deduplicated_data AS (
    SELECT
        reg_code,
        MAX(legal_form) AS legal_form,
        MAX(status) AS status,
        MAX(name) AS name,
        MAX(type) AS type,
        MAX(emtak) AS emtak
    FROM entity_data
    GROUP BY reg_code
)

SELECT
    ROW_NUMBER() OVER () AS entity_id,
    dd.*
FROM deduplicated_data dd
