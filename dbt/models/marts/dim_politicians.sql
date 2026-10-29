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

-- 1. Deduplicate sitting members: Exactly 1 row per member_id
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
        website_url,
        campaign_url,
        image_url
    from {{ ref('stg_members') }}
    qualify row_number() over (partition by member_id order by member_id) = 1
),

-- 2. Deduplicate FEC mapping: Exactly 1 candidate mapping per bioguide_id
deduped_fec_map as (
    select
        bioguide_id,
        max(fec_candidate_id) as fec_candidate_id
    from {{ source('congress', 'member_fec_candidate_mapping') }}
    group by bioguide_id
),

-- 3. Deduplicate FEC summary: Exactly 1 financial aggregate row per fec_candidate_id
deduped_fec_summary as (
    select
        fec_candidate_id,
        max(total_receipts) as total_receipts
    from {{ source('congress', 'politician_fec_summary') }}
    group by fec_candidate_id
),

-- 4. Deduplicate candidates: Strictly 1 row per candidate_id
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
    qualify row_number() over (partition by candidate_id order by first_file_date desc nulls last) = 1
)

-- INCUMBENTS (Members)
select
    members.member_id as id,
    members.member_id as bioguide_id,
    fec_map.fec_candidate_id as fec_candidate_id,
    members.committee_id as committee_id,
    members.member_name as name,
    members.state_code as state,
    members.chamber as chamber,
    members.district_number as district,
    members.party_name as party,
    members.geoid_cd as geoid_cd,
    members.website_url as website,
    members.campaign_url as campaign,
    members.image_url as imageUrl,
    true as is_incumbent,
    'Incumbent' as status,

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
         left join deduped_fec_map as fec_map
                   on members.member_id = fec_map.bioguide_id
         left join deduped_fec_summary as s
                   on fec_map.fec_candidate_id = s.fec_candidate_id

union all

-- CHALLENGERS (Candidates)
select
    candidates.candidate_id as id,
    cast(null as varchar) as bioguide_id,
    candidates.candidate_id as fec_candidate_id,
    candidates.committee_id as committee_id,
    candidates.candidate_name as name,
    candidates.state_code as state,
    candidates.chamber as chamber,
    candidates.district_number as district,
    candidates.party_name as party,
    candidates.geoid_cd as geoid_cd,
    cast(null as varchar) as website,
    candidates.campaign_url as campaign,
    cast(null as varchar) as imageUrl,
    false as is_incumbent,
    'Candidate' as status,

    0 as sponsored_count,
    0 as cosponsored_count,
    0 as enacted_count,
    cast(null as double) as pass_rate,

    cast(candidates.first_file_date as date) as first_file_date,
    coalesce(s.total_receipts, 0.0) as total_receipts

from deduped_candidates as candidates
         left join deduped_fec_summary as s
                   on candidates.candidate_id = s.fec_candidate_id
where coalesce(candidates.challenge_code, '') != 'I'
  and candidates.candidate_id not in (
      select fec_candidate_id
      from deduped_fec_map
      where fec_candidate_id is not null
  )