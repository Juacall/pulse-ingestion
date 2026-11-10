-- models/marts/mart_elections.sql
{{ config(materialized='table') }}

WITH candidate_pool AS (
    SELECT
        p.id AS candidate_key,
        p.bioguide_id,
        p.fec_candidate_id,
        p.name AS candidate_name,
        p.party,
        p.state AS state_code,
        p.chamber AS office_sought,
        LPAD(COALESCE(p.district, '00'), 2, '0') AS district,
        p.is_incumbent,
        p.total_receipts,
        p.campaign AS campaign_url
    FROM {{ ref('dim_politicians') }} p
),

elections_joined AS (
    SELECT
        e.election_id,
        e.seat_id,
        e.election_cycle,
        e.office_sought,
        e.district,
        e.state_code,
        e.status AS election_status,

        c.candidate_key AS politician_id,
        c.bioguide_id,
        c.fec_candidate_id,
        c.candidate_name,
        c.party,
        c.total_receipts,
        c.campaign_url,

        -- 1. Was candidate an incumbent during race
        c.is_incumbent AS is_incumbent_during_race,

        -- 2. Does candidate currently hold the seat today in fct_seat_tenures
        CASE
            WHEN st_current.politician_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_currently_in_seat,

        -- 3. Winner Determination (Fallback to active seat tenure if no vote API)
        CASE
            WHEN st_current.politician_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_winner

    FROM {{ ref('fct_elections') }} e
    INNER JOIN candidate_pool c
        ON e.state_code = c.state_code
       AND e.office_sought = c.office_sought
       AND e.district = c.district
    -- Check active seat tenure today
    LEFT JOIN {{ ref('fct_seat_tenures') }} st_current
        ON e.seat_id = st_current.seat_id
       AND c.bioguide_id = st_current.politician_id
       AND st_current.is_current_incumbent = TRUE
)

SELECT * FROM elections_joined
ORDER BY election_cycle DESC, total_receipts DESC