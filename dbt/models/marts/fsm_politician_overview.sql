{{ config(
    materialized='table',
    unique_key='id',
    indexes=[
        {'columns': ['state', 'isIncumbent']},
        {'columns': ['party']},
        {'columns': ['bioguide_id']}
    ]
) }}

WITH politician_dim AS (
    SELECT
        id,
        bioguide_id,
        name,
        state,
        chamber,
        district,
        party,
        website,
        campaign,
        imageUrl,
        is_incumbent AS isIncumbent,  -- Alias it right away here
        status,
        sponsored_count,
        cosponsored_count,
        enacted_count,
        pass_rate,
        total_receipts,
        first_file_date
    FROM main.dim_politicians
),

attendance AS (
    SELECT
        member_id,
        total_eligible_votes,
        missed_votes,
        present_abstain_votes
    FROM main.mart_member_attendance
),

loyalty AS (
    SELECT
        member_id,
        party_loyalty_pct
    FROM main.mart_party_loyalty
)

SELECT
    p.id                                AS id,
    p.bioguide_id                       AS bioguide_id,
    p.name                              AS name,
    p.state                             AS state,
    p.chamber                           AS chamber,
    p.district                          AS district,
    p.party                             AS party,
    p.website                           AS website,
    p.campaign                          AS campaign,
    COALESCE(p.imageUrl, '')            AS imageUrl,
    p.isIncumbent                       AS isIncumbent,
    p.status                            AS status,
    COALESCE(p.sponsored_count, 0)      AS sponsoredCount,
    COALESCE(p.cosponsored_count, 0)    AS cosponsoredCount,
    COALESCE(p.enacted_count, 0)        AS enactedCount,
    COALESCE(p.pass_rate, 0.0)          AS passRate,
    COALESCE(p.total_receipts, 0.0)     AS totalReceipts,
    CAST(p.first_file_date AS VARCHAR)  AS firstFileDate,

    -- Pre-calculated Roll Call Votes Object (or NULL if non-incumbent / no votes)
    CASE
        WHEN p.isIncumbent AND COALESCE(att.total_eligible_votes, 0) > 0 THEN
            STRUCT_PACK(
                    totalVotes := att.total_eligible_votes,
                    votesCast := (att.total_eligible_votes - COALESCE(att.missed_votes, 0)),
                    missedVotes := COALESCE(att.missed_votes, 0),
                    presentVotes := COALESCE(att.present_abstain_votes, 0),
                    attendancePercentage := ROUND(
                    ((att.total_eligible_votes - COALESCE(att.missed_votes, 0))::DOUBLE / att.total_eligible_votes::DOUBLE) * 100.0,
                    2
                )
            )
        ELSE NULL
        END AS rollCallVotes,

    -- Loyalty Percentage (Rounded to 1 decimal place)
    CASE
        WHEN p.isIncumbent AND pl.party_loyalty_pct IS NOT NULL
            THEN ROUND(CAST(pl.party_loyalty_pct AS DOUBLE), 1)
        ELSE NULL
        END AS votesWithParty

FROM politician_dim AS p
         LEFT JOIN attendance AS att
                   ON p.bioguide_id = att.member_id AND p.isIncumbent = TRUE
         LEFT JOIN loyalty AS pl
                   ON p.bioguide_id = pl.member_id AND p.isIncumbent = TRUE