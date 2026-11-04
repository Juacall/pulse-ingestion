-- dbt/models/marts/mart_recent_bills.sql
WITH recent_sponsored_bills AS (
    SELECT DISTINCT
        b.congress,
        b.bill_type,
        b.bill_number,
        b.bill_title,
        b.bill_policy_area,
        b.bill_summary,
        b.bill_introduced_date,
        b.bill_latest_action_date,
        b.bill_latest_action_text,
        b.member_id AS sponsor_bioguide_id,
        m.name AS sponsor_name,
        m.party AS sponsor_party,
        m.state AS sponsor_state,
        m.chamber
    FROM {{ ref('stg_bills') }} b
    LEFT JOIN {{ ref('dim_politicians') }} m
ON b.member_id = m.bioguide_id
WHERE b.member_relationship = 'sponsor'
  AND b.bill_introduced_date IS NOT NULL
    )
SELECT
    CONCAT(bill_type, bill_number, '-', congress) AS bill_id,
    CONCAT(UPPER(bill_type), ' ', bill_number) AS bill_number,
    congress,
    chamber,
    bill_title,
    bill_policy_area,
    bill_summary,
    bill_introduced_date,
    bill_latest_action_date,
    bill_latest_action_text,
    sponsor_bioguide_id,
    sponsor_name,
    sponsor_party,
    sponsor_state
FROM recent_sponsored_bills
ORDER BY bill_introduced_date DESC