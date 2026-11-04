-- dbt/models/staging/stg_bills.sql
{{ config(materialized = 'view') }}

select
    cast(congress as integer)               as congress,
    upper(trim(bill_type))                  as bill_type,
    trim(bill_number)                       as bill_number,
    trim(policy_area)                       as bill_policy_area,
    upper(trim(bill_type))
        || '-' || trim(bill_number)         as bill_id,
    trim(title)                             as bill_title,
    trim(summary)                           as bill_summary,
    trim(member_id)                         as member_id,
    lower(trim(relationship))               as member_relationship,

    -- Explicitly derive chamber from bill_type prefix
    case
        when upper(trim(bill_type)) in ('S', 'SRES', 'SJRES', 'SCONRES') then 'Senate'
        when upper(trim(bill_type)) in ('HR', 'HRES', 'HJRES', 'HCONRES') then 'House'
        else 'Unknown'
        end                                     as chamber,

    -- Human-readable bill type label (added Senate types)
    case upper(trim(bill_type))
        when 'HR'      then 'House Bill'
        when 'HRES'    then 'House Resolution'
        when 'HJRES'   then 'House Joint Resolution'
        when 'HCONRES' then 'House Concurrent Resolution'
        when 'S'       then 'Senate Bill'
        when 'SRES'    then 'Senate Resolution'
        when 'SJRES'   then 'Senate Joint Resolution'
        when 'SCONRES' then 'Senate Concurrent Resolution'
        else upper(trim(bill_type))
        end                                     as bill_type_label,

    try_cast(introduced_date as date)       as bill_introduced_date,
    try_cast(latest_action_date as date)    as bill_latest_action_date,
    nullif(trim(latest_action_text), '')    as bill_latest_action_text,

    case
        when latest_action_text is null or trim(latest_action_text) = ''
            then 'Unknown'
        when lower(latest_action_text) like '%became public law%'
            or lower(latest_action_text) like '%became private law%'
            or lower(latest_action_text) like '%signed by president%'
            then 'Became Law'
        when lower(latest_action_text) like '%vetoed%'
            then 'Vetoed'
        when lower(latest_action_text) like '%passed senate%'
            or lower(latest_action_text) like '%agreed to in senate%'
            then 'Passed Senate'
        when lower(latest_action_text) like '%passed house%'
            or lower(latest_action_text) like '%agreed to in house%'
            or lower(latest_action_text) like '%passed/agreed to in house%'
            then 'Passed House'
        when lower(latest_action_text) like '%referred to%'
            or lower(latest_action_text) like '%committee%'
            then 'In Committee'
        when lower(latest_action_text) like '%introduced%'
            then 'Introduced'
        else 'Other'
        end                                     as bill_status
from {{ source('congress', 'raw_bills') }}
where
    member_id is not null
  and trim(member_id) != ''
    and bill_type is not null
    and bill_number is not null
    and trim(bill_number) != ''
    and congress is not null
    and upper(trim(bill_type)) in (
        'HR', 'HRES', 'HJRES', 'HCONRES',
        'S', 'SRES', 'SJRES', 'SCONRES'
    )
    and lower(trim(relationship)) in ('sponsor', 'cosponsor')