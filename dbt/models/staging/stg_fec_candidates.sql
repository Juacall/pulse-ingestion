-- models/staging/stg_fec_candidates.sql
{{ config(materialized='view') }}

SELECT
    candidate_id,
    name                AS candidate_name,
    state               AS state_code,
    -- Standardize chamber representation
    CASE
        WHEN UPPER(TRIM(chamber)) IN ('H', 'HOUSE') THEN 'House'
        WHEN UPPER(TRIM(chamber)) IN ('S', 'SENATE') THEN 'Senate'
        ELSE chamber
        END AS chamber,
    LPAD(COALESCE(district, '00'), 2, '0') AS district_number,
    party               AS party_name,
    geoid_cd,
    campaign_url,
    committee_id,
    candidate_status,
    UPPER(TRIM(incumbent_challenge)) AS challenge_code, -- 'I' = Incumbent, 'C' = Challenger, 'O' = Open Seat
    election_year,
    fec_update_date,
    first_file_date,
    row_hash,
    ingested_at
FROM {{ source('congress', 'raw_fec_candidates') }}