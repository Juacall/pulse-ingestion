-- stg_amendments.sql
--
-- Clean and rename raw_amendments.
-- One row per amendment — amendments without a valid bill reference are dropped.
{{ config(materialized = 'view') }}
select
    cast(congress as integer)               as congress,
    upper(trim(bill_type))                  as bill_type,
    trim(bill_number)                       as bill_number,
    upper(trim(bill_type))
        || '-' || trim(bill_number)         as bill_id,     -- joins to stg_bills.bill_id
    trim(amendment_number)                  as amendment_number,
    upper(trim(amendment_type))             as amendment_type,
    coalesce(
        nullif(upper(trim(amendment_type)), ''), 'AMDT'
    ) || '-' || trim(amendment_number)      as amendment_id,
    coalesce(
        nullif(trim(description), ''),
        nullif(trim(purpose), ''),
        'Amendment ' || trim(amendment_number)
    )                                       as amendment_title,
    nullif(trim(sponsor_id), '')            as sponsor_id   -- null if unknown or outside target districts
from {{ source('congress', 'raw_amendments') }}
where
    amendment_number is not null
    and trim(amendment_number) != ''
    and bill_type is not null
    and bill_number is not null
    and trim(bill_number) != ''
    and congress is not null