
{{ config(materialized='table', unique_key='id') }}

with member_activity as (
    select
        member_id,
        count(distinct case when activity_type = 'bill' and relationship = 'sponsor' then bill_id end) as sponsored_count,
        count(distinct case when activity_type = 'bill' and relationship = 'cosponsor' then bill_id end) as cosponsored_count,
        count(distinct case when activity_type = 'bill' and relationship = 'sponsor' and lower(bill_status) like '%became law%' then bill_id end) as enacted_count
    from {{ ref('int_legislative_activity') }}
    group by member_id
)

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
    true as is_incumbent,
    'Active' as status,

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
        end as pass_rate

from {{ ref('stg_members') }} as members
left join member_activity as activity
on members.member_id = activity.member_id  -- Joined on member_id
    left join {{ source('congress', 'member_fec_candidate_mapping') }} as fec_map
    on members.member_id = fec_map.bioguide_id

union all

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
    false as is_incumbent,
    'Candidate' as status,

    0 as sponsored_count,
    0 as cosponsored_count,
    0 as enacted_count,
    cast(null as double) as pass_rate

from {{ ref('stg_fec_candidates') }} as candidates
where candidates.challenge_code != 'I'