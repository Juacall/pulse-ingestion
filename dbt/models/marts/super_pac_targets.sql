{{ config(materialized='table', unique_key='id') }}

SELECT
    super_pac_id,
    super_pac_name,
    candidate_name,
    fec_candidate_id,
    stance,
    cycle,
    SUM(amount) AS total_amount,
    COUNT(*) AS transaction_count
FROM {{ source('congress', 'stg_fec_independent_expenditures') }}
WHERE super_pac_id LIKE 'C%'
  AND candidate_name IS NOT NULL
GROUP BY
    super_pac_id,
    super_pac_name,
    candidate_name,
    fec_candidate_id,
    stance,
    cycle