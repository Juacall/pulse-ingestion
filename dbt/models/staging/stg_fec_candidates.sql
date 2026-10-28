-- models/staging/stg_fec_candidates.sql
{{ config(materialized = 'view') }}

SELECT
    candidate_id,
    name                AS candidate_name,
    state               AS state_code,
    chamber,
    district            AS district_number,
    party               AS party_name,
    geoid_cd,
    campaign_url,
    committee_id,
    incumbent_challenge AS challenge_code,
    election_year,
    fec_update_date,
    first_file_date,
    row_hash,
    ingested_at
FROM {{ source('congress', 'raw_fec_candidates') }}
-- Exclude Incumbents ('I') because they are already ingested via stg_members
WHERE incumbent_challenge != 'I'