-- dbt/models/marts/mart_recently_passed_bills.sql
{{ config(materialized='table') }}

WITH passed_bills AS (
    SELECT
        b.congress,
        b.bill_type,
        b.bill_number,
        b.bill_title,
        b.bill_policy_area,
        b.bill_summary,
        b.bill_introduced_date,
        b.bill_latest_action_date,
        b.bill_latest_action_text,
        b.bill_status,
        b.chamber,
        b.member_id AS sponsor_bioguide_id,
        ROW_NUMBER() OVER (
            PARTITION BY b.congress, b.bill_type, b.bill_number
            ORDER BY b.bill_latest_action_date DESC
        ) AS rn
    FROM {{ ref('stg_bills') }} b
    WHERE b.member_relationship = 'sponsor'
      AND b.bill_status IN ('Passed House', 'Passed Senate', 'Became Law')
)
SELECT
    CONCAT(b.bill_type, b.bill_number, '-', b.congress) AS bill_id,
    CONCAT(UPPER(b.bill_type), ' ', b.bill_number) AS bill_number,
    b.congress,
    b.chamber,
    b.bill_title,
    b.bill_policy_area,
    b.bill_summary,
    b.bill_introduced_date,
    b.bill_latest_action_date,
    b.bill_latest_action_text,
    b.bill_status,
    b.sponsor_bioguide_id,
    m.name AS sponsor_name,
    m.party AS sponsor_party,
    m.state AS sponsor_state
FROM passed_bills b
         LEFT JOIN {{ ref('dim_politicians') }} m
ON b.sponsor_bioguide_id = m.bioguide_id
WHERE b.rn = 1
ORDER BY b.bill_latest_action_date DESC