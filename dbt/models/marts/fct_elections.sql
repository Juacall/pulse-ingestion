-- models/marts/fct_elections.sql
{{ config(materialized='table') }}

WITH candidate_races AS (
    -- 1. Contests generated from active challenger / open-seat filings
    SELECT
        election_year AS election_cycle,
        state_code,
        CASE
            WHEN UPPER(TRIM(chamber)) IN ('H', 'HOUSE') THEN 'H'
            WHEN UPPER(TRIM(chamber)) IN ('S', 'SENATE') THEN 'S'
            ELSE UPPER(TRIM(chamber))
        END AS office_code,
        LPAD(COALESCE(district_number, '00'), 2, '0') AS district
    FROM {{ ref('stg_fec_candidates') }}
    WHERE state_code IS NOT NULL
      AND chamber IS NOT NULL

    UNION

    -- 2. Contests generated from sitting incumbents in dim_politicians
    SELECT
        2026 AS election_cycle,
        state AS state_code,
        CASE
            WHEN UPPER(TRIM(chamber)) IN ('H', 'HOUSE') THEN 'H'
            WHEN UPPER(TRIM(chamber)) IN ('S', 'SENATE') THEN 'S'
            ELSE UPPER(TRIM(chamber))
        END AS office_code,
        LPAD(COALESCE(district, '00'), 2, '0') AS district
    FROM {{ ref('dim_politicians') }}
    WHERE is_incumbent = TRUE
      AND state IS NOT NULL
      AND chamber IS NOT NULL
),

formatted_races AS (
    SELECT DISTINCT
        election_cycle,
        state_code,
        CASE
            WHEN office_code = 'H' THEN 'House'
            WHEN office_code = 'S' THEN 'Senate'
            ELSE office_code
        END AS office_sought,
        district,
        -- Construct seat_id matching dim_seats conventions
        CASE
            WHEN office_code = 'H' THEN CONCAT(state_code, '-H-', district)
            -- For Senate, map to seat_id or default to state level
            ELSE CONCAT(state_code, '-S-1')
        END AS seat_id
    FROM candidate_races
)

SELECT
    CONCAT(election_cycle, '_', state_code, '_', IF(office_sought = 'House', 'H', 'S'), '_', district) AS election_id,
    seat_id,
    election_cycle,
    state_code,
    office_sought,
    district,
    'Certified' AS status
FROM formatted_races