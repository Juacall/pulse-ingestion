{{ config(materialized='table') }}

with candidate_races as (
    -- 1. Contests generated from active challenger / open-seat filings
    select
        election_year as election_cycle,
        state_code,
        case
            when upper(trim(chamber)) in ('H', 'HOUSE') then 'H'
            when upper(trim(chamber)) in ('S', 'SENATE') then 'S'
            else upper(trim(chamber))
        end as office_code,
        lpad(coalesce(district_number, '00'), 2, '0') as district
    from {{ ref('stg_fec_candidates') }}
    where state_code is not null
      and chamber is not null

    union

    -- 2. Contests generated from sitting incumbents in dim_politicians
    select
        2026 as election_cycle,
        state as state_code,
        case
            when upper(trim(chamber)) in ('H', 'HOUSE') then 'H'
            when upper(trim(chamber)) in ('S', 'SENATE') then 'S'
            else upper(trim(chamber))
        end as office_code,
        lpad(coalesce(district, '00'), 2, '0') as district
    from {{ ref('dim_politicians') }}
    where is_incumbent = true
      and state is not null
      and chamber is not null
)

select distinct
    concat(
            election_cycle, '_',
            state_code, '_',
            office_code, '_',
            district
    ) as election_id,
    election_cycle,
    state_code,
    case
        when office_code = 'H' then 'House'
        when office_code = 'S' then 'Senate'
        else office_code
        end as office_sought,
    district
from candidate_races