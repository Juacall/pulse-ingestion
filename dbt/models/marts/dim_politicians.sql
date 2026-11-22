{{ config(materialized='table', unique_key='id') }}

with member_activity as (
    select
        member_id,
        count(distinct case when activity_type = 'bill' and relationship = 'sponsor' then bill_id end) as sponsored_count,
        count(distinct case when activity_type = 'bill' and relationship = 'cosponsor' then bill_id end) as cosponsored_count,
        count(distinct case when activity_type = 'bill' and relationship = 'sponsor' and lower(bill_status) like '%became law%' then bill_id end) as enacted_count
    from {{ ref('int_legislative_activity') }}
    group by member_id
),

-- 1. Deduplicate sitting members: Pick the most recent record deterministically
deduped_members as (
    select
        member_id,
        committee_id,
        member_name,
        state_code,
        chamber,
        district_number,
        party_name,
        geoid_cd,
        congress,
        website_url,
        campaign_url,
        image_url,
        start_year,
        end_year
    from {{ ref('stg_members') }}
    qualify row_number() over (
        partition by member_id
        order by congress desc, start_year desc nulls last
    ) = 1
),

-- 2. Deduplicate candidates: Strictly 1 row per candidate_id from bulk staging
deduped_candidates as (
    select
        candidate_id,
        committee_id,
        candidate_name,
        state_code,
        chamber,
        district_number,
        party_name,
        geoid_cd,
        campaign_url,
        challenge_code,
        first_file_date
    from {{ ref('stg_fec_candidates') }}
    qualify row_number() over (
        partition by candidate_id
        order by first_file_date desc nulls last
    ) = 1
),

-- 3. Strict 1-to-1 matching between Incumbents and FEC Candidates
-- Matches on State + Chamber + District + Last Name + First Name prefix (handles "First Last" vs "LAST, FIRST")
candidate_member_matches as (
    select
        m.member_id,
        c.candidate_id as fec_candidate_id,
        row_number() over (
            partition by m.member_id
            order by
                case when m.committee_id is not null and m.committee_id = c.committee_id then 0 else 1 end,
                case when c.challenge_code = 'I' then 0 else 1 end,
                c.first_file_date desc nulls last,
                c.candidate_id desc
        ) as member_rn,
        row_number() over (
            partition by c.candidate_id
            order by
                case when m.committee_id is not null and m.committee_id = c.committee_id then 0 else 1 end,
                case when c.challenge_code = 'I' then 0 else 1 end,
                m.congress desc,
                m.member_id asc
        ) as candidate_rn
    from deduped_members m
    inner join deduped_candidates c
        on (
            -- Direct committee_id match (exact 100% certainty)
            (m.committee_id is not null and trim(m.committee_id) != '' and trim(m.committee_id) = trim(c.committee_id))
            or (
                upper(trim(m.state_code)) = upper(trim(c.state_code))
                and upper(trim(m.chamber)) = upper(trim(c.chamber))
                and (
                    upper(trim(m.chamber)) = 'SENATE'
                    or lpad(coalesce(trim(cast(m.district_number as varchar)), '00'), 2, '0')
                     = lpad(coalesce(trim(cast(c.district_number as varchar)), '00'), 2, '0')
                )
                and upper(trim(split_part(c.candidate_name, ',', 1))) = upper(trim(
                    case
                        when strpos(m.member_name, ',') > 0 then split_part(m.member_name, ',', 1)
                        else split_part(regexp_replace(m.member_name, '\s+(Jr\.?|Sr\.?|II|III|IV)$', '', 'i'), ' ', -1)
                    end
                ))
            )
        )
),

inferred_fec_map as (
    select
        member_id,
        fec_candidate_id
    from candidate_member_matches
    where member_rn = 1
      and candidate_rn = 1
),

-- 4. Deduplicate FEC summary: Exactly 1 financial aggregate row per fec_candidate_id
deduped_fec_summary as (
    select
        fec_candidate_id,
        max(total_receipts) as total_receipts
    from {{ source('congress', 'politician_fec_summary') }}
    group by fec_candidate_id
)

-- INCUMBENTS (Sitting Members)
select
    members.member_id as id,
    members.member_id as bioguide_id,
    fec_map.fec_candidate_id as fec_candidate_id,
    members.committee_id as committee_id,
    members.member_name as name,
    members.state_code as state,
    members.congress as congress,
    members.chamber as chamber,
    members.district_number as district,
    members.party_name as party,
    members.geoid_cd as geoid_cd,
    members.website_url as website,
    members.campaign_url as campaign,
    members.image_url as imageUrl,
    true as is_incumbent,
    'Incumbent' as status,

    members.start_year as start_year,
    members.end_year as end_year,

    coalesce(activity.sponsored_count, 0) as sponsored_count,
    coalesce(activity.cosponsored_count, 0) as cosponsored_count,
    coalesce(activity.enacted_count, 0) as enacted_count,

    case
        when coalesce(activity.sponsored_count, 0) > 0
            then round(
                cast(coalesce(activity.enacted_count, 0) as double) * 100.0
                    / activity.sponsored_count,
                1
                 )
        else cast(null as double)
        end as pass_rate,

    cast(null as date) as first_file_date,
    coalesce(s.total_receipts, 0.0) as total_receipts

from deduped_members as members
         left join member_activity as activity
                   on members.member_id = activity.member_id
         left join inferred_fec_map as fec_map
                   on members.member_id = fec_map.member_id
         left join deduped_fec_summary as s
                   on fec_map.fec_candidate_id = s.fec_candidate_id

union all

-- CHALLENGERS (Candidates competing for seats who are not incumbents)
select
    candidates.candidate_id as id,
    cast(null as varchar) as bioguide_id,
    candidates.candidate_id as fec_candidate_id,
    candidates.committee_id as committee_id,
    candidates.candidate_name as name,
    candidates.state_code as state,
    cast(0 as integer) as congress,
    candidates.chamber as chamber,
    candidates.district_number as district,
    candidates.party_name as party,
    candidates.geoid_cd as geoid_cd,
    cast(null as varchar) as website,
    candidates.campaign_url as campaign,
    cast(null as varchar) as imageUrl,
    false as is_incumbent,
    'Candidate' as status,

    cast(null as integer) as start_year,
    cast(null as integer) as end_year,

    0 as sponsored_count,
    0 as cosponsored_count,
    0 as enacted_count,
    cast(null as double) as pass_rate,

    cast(candidates.first_file_date as date) as first_file_date,
    coalesce(s.total_receipts, 0.0) as total_receipts

from deduped_candidates as candidates
         left join deduped_fec_summary as s
                   on candidates.candidate_id = s.fec_candidate_id
where not exists (
    select 1
    from inferred_fec_map m
    where m.fec_candidate_id = candidates.candidate_id
)