-- models/intermediate/int_politicians.sql
{{ config(materialized = 'view', unique_key = 'politician_id') }}

-- Sitting Members (Incumbents)
SELECT
    m.member_id                             AS politician_id,
    m.member_id                             AS bioguide_id,
    NULL::VARCHAR                           AS fec_candidate_id,
    m.committee_id                          AS committee_id,
    m.member_name                           AS name,
    m.state_code                            AS state,
    m.chamber                               AS chamber,
    m.district_number                       AS district,
    m.party_name                            AS party,
    m.geoid_cd                              AS geoid_cd,
    m.website_url                           AS website,
    m.campaign_url                          AS campaign,
    TRUE                                    AS is_incumbent,
    'active_member'                         AS status
FROM {{ ref('stg_members') }} m

UNION ALL

-- Non-Incumbent Candidates from OpenFEC
SELECT
    c.candidate_id                          AS politician_id,
    NULL::VARCHAR                           AS bioguide_id,
    c.candidate_id                          AS fec_candidate_id,
    c.committee_id                          AS committee_id,
    c.candidate_name                        AS name,
    c.state_code                            AS state,
    c.chamber                               AS chamber,
    c.district_number                       AS district,
    c.party_name                            AS party,
    c.geoid_cd                              AS geoid_cd,
    NULL::VARCHAR                           AS website,
    c.campaign_url                          AS campaign,
    FALSE                                   AS is_incumbent,
    'candidate'                             AS status
FROM {{ ref('stg_fec_candidates') }} c
WHERE c.challenge_code != 'I' -- Exclude incumbents to avoid duplication