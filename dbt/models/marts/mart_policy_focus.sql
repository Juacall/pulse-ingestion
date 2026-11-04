-- Groups lawmaker bill activity by Policy Area
SELECT
    b.member_id,
    COALESCE(NULLIF(TRIM(rb.policy_area), ''), 'General / Uncategorized') AS policy_area,
    COUNT(DISTINCT b.bill_id) AS bill_count,
    ROUND(
            (COUNT(DISTINCT b.bill_id)::FLOAT / SUM(COUNT(DISTINCT b.bill_id)) OVER (PARTITION BY b.member_id)) * 100,
            1
    ) AS percentage_of_total
FROM {{ ref('stg_bills') }} b
JOIN {{ source('congress', 'raw_bills') }} rb
ON b.congress = rb.congress
    AND b.bill_type = rb.bill_type
    AND b.bill_number = rb.bill_number
GROUP BY b.member_id, COALESCE(NULLIF(TRIM(rb.policy_area), ''), 'General / Uncategorized')