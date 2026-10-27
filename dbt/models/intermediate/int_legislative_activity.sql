{{ config(materialized = 'table') }}

with bills as (
    select
        member_id,
        bill_id                             as bill_id,
        bill_title                          as bill_title,
        cast(null as varchar)               as amendment_id,
        cast(null as varchar)               as amendment_title,
        cast(null as varchar)               as amendment_sponsor_id,
        'bill'                              as activity_type,
        member_relationship                 as relationship,
        congress,
        bill_type,
        bill_number,
        bill_status                         as bill_status,
        bill_introduced_date                as bill_introduced_date,
        bill_latest_action_date             as bill_latest_action_date,
        bill_latest_action_text             as bill_latest_action_text
    from {{ ref('stg_bills') }}
),

amendments as (
    select
        b.member_id,
        b.bill_id                           as bill_id,
        b.bill_title                        as bill_title,
        a.amendment_id                      as amendment_id,
        a.amendment_title                   as amendment_title,
        a.sponsor_id                        as amendment_sponsor_id,
        'amendment'                         as activity_type,
        case
            when a.sponsor_id = b.member_id then 'amendment_sponsor'
            else 'sponsored bill received amendment'
        end                                 as relationship,
        a.congress,
        a.bill_type,
        a.bill_number,
        b.bill_status                       as bill_status,
        b.bill_introduced_date              as bill_introduced_date,
        b.bill_latest_action_date           as bill_latest_action_date,
        b.bill_latest_action_text           as bill_latest_action_text
    from {{ ref('stg_amendments') }} as a
    inner join {{ ref('stg_bills') }} as b
        on  a.congress    = b.congress
        and a.bill_type   = b.bill_type
        and a.bill_number = b.bill_number
),

unmerged as (
    select * from bills
    union all
    select * from amendments
)

select
    md5(
            coalesce(cast(member_id as varchar), '') || '-' ||
            coalesce(cast(relationship as varchar), '') || '-' ||
            coalesce(cast(activity_type as varchar), '') || '-' ||
            coalesce(cast(coalesce(amendment_id, bill_id) as varchar), '')
    ) as legislative_activity_sk,

    member_id,
    bill_id,
    bill_title,
    amendment_id,
    amendment_title,
    amendment_sponsor_id,
    activity_type,
    relationship,
    congress,
    bill_type,
    bill_number,
    bill_status,
    bill_introduced_date,
    bill_latest_action_date,
    bill_latest_action_text
from unmerged