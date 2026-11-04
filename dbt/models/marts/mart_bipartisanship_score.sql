-- Calculates percentage of sponsored bills that received cross-party cosponsorship
WITH primary_sponsors AS (
    SELECT
        b.bill_id,
        b.member_id AS sponsor_id,
        m.party AS sponsor_party
    FROM {{ ref('stg_bills') }} b
    JOIN {{ source('congress', 'raw_members') }} m ON b.member_id = m.bioguide_id
WHERE b.member_relationship = 'sponsor'
    ),
    cosponsors AS (
SELECT
    b.bill_id,
    b.member_id AS cosponsor_id,
    m.party AS cosponsor_party
FROM {{ ref('stg_bills') }} b
    JOIN {{ source('congress', 'raw_members') }} m ON b.member_id = m.bioguide_id
WHERE b.member_relationship = 'cosponsor'
    ),
    bipartisan_bills AS (
SELECT DISTINCT
    ps.sponsor_id,
    ps.bill_id
FROM primary_sponsors ps
    JOIN cosponsors cs ON ps.bill_id = cs.bill_id
WHERE ps.sponsor_party != cs.cosponsor_party
    )
SELECT
    ps.sponsor_id AS member_id,
    COUNT(DISTINCT ps.bill_id) AS total_sponsored_bills,
    COUNT(DISTINCT bb.bill_id) AS bipartisan_sponsored_bills,
    ROUND(
            (COUNT(DISTINCT bb.bill_id)::FLOAT / NULLIF(COUNT(DISTINCT ps.bill_id), 0)) * 100,
            1
    ) AS bipartisanship_score
FROM primary_sponsors ps
         LEFT JOIN bipartisan_bills bb ON ps.sponsor_id = bb.sponsor_id AND ps.bill_id = bb.bill_id
GROUP BY ps.sponsor_id