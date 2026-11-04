-- Aggregates legislative stage progression per lawmaker
SELECT
    b.member_id,
    b.member_relationship,
    COUNT(DISTINCT b.bill_id) AS total_bills,
    COUNT(DISTINCT CASE WHEN b.bill_status != 'Unknown' THEN b.bill_id END) AS introduced_count,
    COUNT(DISTINCT CASE WHEN b.bill_status IN ('In Committee', 'Passed House', 'Passed Senate', 'Became Law') THEN b.bill_id END) AS in_committee_count,
    COUNT(DISTINCT CASE WHEN b.bill_status IN ('Passed House', 'Passed Senate', 'Became Law') THEN b.bill_id END) AS passed_chamber_count,
    COUNT(DISTINCT CASE WHEN b.bill_status = 'Became Law' THEN b.bill_id END) AS enacted_count
FROM {{ ref('stg_bills') }} b
GROUP BY b.member_id, b.member_relationship