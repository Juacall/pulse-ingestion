-- models/staging/stg_fec_candidates.sql
{{ config(materialized='view') }}

select
    candidate_id,
    name                as candidate_name,
    state               as state_code,
    chamber,
    district            as district_number,
    party               as party_name,
    geoid_cd,
    campaign_url,
    committee_id,
    candidate_status,
    incumbent_challenge as challenge_code,
    election_year,
    fec_update_date,
    first_file_date,
    row_hash,
    ingested_at
from {{ source('congress', 'raw_fec_candidates') }}
-- Exclude Incumbents ('I') because they are already ingested via stg_members
where coalesce(incumbent_challenge, '') != 'I'