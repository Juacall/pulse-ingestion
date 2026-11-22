{{ config(
    materialized='table',
    unique_key='politician_id',
    indexes=[
        {'columns': ['state', 'district']},
        {'columns': ['state', 'is_incumbent']}
    ]
) }}

WITH politicians AS (
    SELECT
        id AS politician_id,
        bioguide_id,
        fec_candidate_id,
        name AS politician_name,
        UPPER(TRIM(state)) AS state,
        chamber,
        district,
        party,
        is_incumbent,
        status
    FROM {{ ref('dim_politicians') }}
),

fec_summary AS (
    SELECT
        fec_candidate_id,
        politician_id,
        MAX(total_receipts) AS total_receipts,
        MAX(individual_unitemized) AS individual_unitemized,
        MAX(individual_itemized) AS individual_itemized,
        MAX(pac_contributions) AS pac_contributions,
        MAX(individual_contributions) AS individual_contributions,
        MAX(grassroots_percentage) AS grassroots_percentage,
        MAX(coverage_end_date) AS coverage_end_date
    FROM {{ source('congress', 'politician_fec_summary') }}
    GROUP BY fec_candidate_id, politician_id
)

SELECT
    p.politician_id,
    p.bioguide_id,
    p.fec_candidate_id,
    p.politician_name,
    p.state,
    p.chamber,
    p.district,
    p.party,
    p.is_incumbent,
    p.status,

    -- Standardized financial columns (never NULL)
    ROUND(COALESCE(s.total_receipts, 0.0), 2) AS total_receipts,
    ROUND(COALESCE(s.individual_unitemized, 0.0), 2) AS grassroots_dollars,
    ROUND(COALESCE(s.individual_itemized, 0.0), 2) AS large_donor_dollars,
    ROUND(COALESCE(s.pac_contributions, 0.0), 2) AS pac_dollars,
    ROUND(COALESCE(s.individual_contributions, 0.0), 2) AS individual_dollars,
    ROUND(
            COALESCE(
                    s.grassroots_percentage,
                    (s.individual_unitemized / NULLIF(s.total_receipts, 0.0)) * 100.0,
                    0.0
            ),
            2
    ) AS grassroots_pct,
    CAST(s.coverage_end_date AS VARCHAR) AS coverage_end_date,
    (s.total_receipts IS NOT NULL) AS has_fec_filing

FROM politicians p
         LEFT JOIN fec_summary s
                   ON (p.fec_candidate_id IS NOT NULL AND p.fec_candidate_id = s.fec_candidate_id)
                       OR (p.fec_candidate_id IS NULL AND p.politician_id = s.politician_id)