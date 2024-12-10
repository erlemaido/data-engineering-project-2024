{{ config(materialized='table') }}

WITH financial_data AS (
    SELECT
        report_id,
        element_eng,
        element_value
    FROM {{ ref('staging_report_financial_data') }}
),

pivoted_values AS (
    SELECT
        report_id,
        MAX(CASE WHEN element_eng = 'CurrentAssets' THEN element_value END) AS current_assets,
        MAX(CASE WHEN element_eng = 'CurrentLiabilities' THEN element_value END) AS current_liabilities,
        MAX(CASE WHEN element_eng = 'NonCurrentAssets' THEN element_value END) AS non_current_assets,
        MAX(CASE WHEN element_eng = 'NonCurrentLiabilities' THEN element_value END) AS non_current_liabilities,
        MAX(CASE WHEN element_eng = 'Revenue' THEN element_value END) AS revenue,
        MAX(CASE WHEN element_eng = 'TotalAnnualPeriodProfitLoss' THEN element_value END) AS profit_loss,
        MAX(CASE WHEN element_eng = 'TotalProfitLoss' THEN element_value END) AS total_profit_loss,
        MAX(CASE WHEN element_eng = 'LaborExpense' THEN element_value END) AS labour_expense,
        MAX(CASE WHEN element_eng = 'DeprecationAndImpairmentLossReversal' THEN element_value END) AS deprecation_impairment,
        MAX(CASE WHEN element_eng = 'CashAndCashEquivalents' THEN element_value END) AS cash,
        MAX(CASE WHEN element_eng = 'Equity' THEN element_value END) AS equity,
        MAX(CASE WHEN element_eng = 'IssuedCapital' THEN element_value END) AS issued_capital,
        MAX(CASE WHEN element_eng = 'Assets' THEN element_value END) AS assets
    FROM financial_data
    GROUP BY report_id
)

SELECT
    ROW_NUMBER() OVER () AS financial_performance_id,
    fyd.fy_report_id,
    pv.current_assets,
    pv.current_liabilities,
    pv.non_current_assets,
    pv.non_current_liabilities,
    pv.revenue,
    pv.profit_loss,
    pv.total_profit_loss,
    pv.labour_expense,
    pv.deprecation_impairment,
    pv.cash,
    pv.equity,
    pv.issued_capital,
    pv.assets
FROM pivoted_values pv
INNER JOIN {{ ref('fy_report_dim') }} fyd
    ON pv.report_id = fyd.fy_report_id