-- models/marts/elections/fct_race_candidates.sql
{{ config(materialized='table') }}

with fec_challengers as (
    -- Non-incumbents (Challengers & Open Seats)
    select
        concat(election_year, '_', candidate_id) as race_candidate_id,
        concat(
            election_year, '_',
            state_code, '_',
            case
                when upper(trim(chamber)) in ('H', 'HOUSE') then 'H'
                when upper(trim(chamber)) in ('S', 'SENATE') then 'S'
                else upper(trim(chamber))
            end, '_',
            lpad(coalesce(district_number, '00'), 2, '0')
        ) as election_id,
        candidate_id as politician_id,
        false as is_incumbent,
        coalesce(candidate_status, 'Candidate') as candidate_status,
        -- Result fields populated via Secretary of State / State election data syncs
        cast(null as integer) as votes_count,
        cast(null as double) as vote_percentage,
        false as is_winner
    from {{ ref('stg_fec_candidates') }}
    where state_code is not null
      and chamber is not null
),

sitting_incumbents as (
    -- Incumbents pulled from dim_politicians
    select
        concat(2026, '_', id) as race_candidate_id,
        concat(
            2026, '_',
            state, '_',
            case
                when upper(trim(chamber)) in ('H', 'HOUSE') then 'H'
                when upper(trim(chamber)) in ('S', 'SENATE') then 'S'
                else upper(trim(chamber))
            end, '_',
            lpad(coalesce(district, '00'), 2, '0')
        ) as election_id,
        id as politician_id,
        true as is_incumbent,
        'Incumbent' as candidate_status,
        -- Result fields populated via Secretary of State / State election data syncs
        cast(null as integer) as votes_count,
        cast(null as double) as vote_percentage,
        false as is_winner
    from {{ ref('dim_politicians') }}
    where is_incumbent = true
      and state is not null
      and chamber is not null
)

select * from fec_challengers
union all
select * from sitting_incumbents