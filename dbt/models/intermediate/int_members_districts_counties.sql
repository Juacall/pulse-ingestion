-- models/intermediate/int_members_districts_counties.sql
-- GRAIN: one row per member x county (Nationwide).

{{ config(materialized = 'table') }}

with house_members as (

    select distinct
        m.member_id,
        m.chamber,
        m.member_name,
        m.state_code,
        m.state_name,
        m.state_fips,
        m.district_number,
        m.party_name,
        m.geoid_cd,
        lpad(cast(census.GEOID_COUNTY_20 as varchar), 5, '0') as county_fips,
        c.county_name,
        m.state_code as county_state
    from {{ ref('stg_members') }} as m
    inner join {{ ref('raw_census__cd11920_county20') }} as census
        on m.geoid_cd = census.GEOID_CD119_20
    left join {{ ref('stg_counties') }} as c
        on lpad(cast(census.GEOID_COUNTY_20 as varchar), 5, '0') = c.county_fips
    where lower(m.chamber) = 'house'

),

senate_members as (

    select distinct
        m.member_id,
        m.chamber,
        m.member_name,
        m.state_code,
        m.state_name,
        m.state_fips,
        m.district_number,  -- NULL for Senators
        m.party_name,
        m.geoid_cd,         -- NULL for Senators
        c.county_fips,
        c.county_name,
        m.state_code as county_state
    from {{ ref('stg_members') }} as m
    inner join {{ ref('raw_census__cd11920_county20') }} as census
        -- Extract state FIPS (first 2 digits of county GEOID) and match state
        on substr(lpad(cast(census.GEOID_COUNTY_20 as varchar), 5, '0'), 1, 2)
            = m.state_fips
    left join {{ ref('stg_counties') }} as c
        on lpad(cast(census.GEOID_COUNTY_20 as varchar), 5, '0') = c.county_fips
    where lower(m.chamber) = 'senate'

)

select * from house_members
union all
select * from senate_members