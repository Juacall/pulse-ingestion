-- models/staging/stg_member_terms.sql
{{ config(materialized='view') }}

WITH raw AS (
    SELECT
        bioguide_id AS politician_id,
        chamber,
        state AS state_code,
        LPAD(COALESCE(district::TEXT, '00'), 2, '0') AS district,
        start_year,
        end_year,
        -- Convert start_year (INTEGER) directly to Congress number
        CAST(119 - ((2025 - start_year) / 2) AS INTEGER) AS congress,
        -- DuckDB MAKE_DATE constructor using integer year, month, day
        MAKE_DATE(start_year, 1, 3) AS start_date,
        CASE
            WHEN end_year IS NOT NULL THEN MAKE_DATE(end_year, 1, 3)
            ELSE NULL
        END AS end_date
    FROM {{ source('congress', 'raw_members') }}
)

SELECT
    politician_id,
    chamber,
    state_code,
    district,
    congress,
    start_year,
    end_year,
    start_date,
    end_date,
    CASE
        WHEN end_year IS NULL OR end_year >= 2026 THEN TRUE
        ELSE FALSE
        END AS is_active_term
FROM raw