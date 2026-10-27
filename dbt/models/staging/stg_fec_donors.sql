{{ config(materialized='view') }}

select
    politician_id,
    cast(cycle as varchar) as cycle,
    amount,
    industry,

    -- Standardize donor display name
    case
        when upper(trim(coalesce(donor_name, ''))) in (
                                                     'NOT EMPLOYED', 'UNEMPLOYED', 'RETIRED', 'SELF EMPLOYED',
                                                     'SELF-EMPLOYED', 'NONE', 'N/A', 'HOMEMAKER', 'STUDENT',
                                                     'INFORMATION REQUESTED', 'REFUND', ''
            ) then 'Non-Corporate / Grassroots'
        else (trim(donor_name))
        end as donor_name,

    -- Set the non-corporate boolean flag
    case
        when upper(trim(coalesce(donor_name, ''))) in (
                                                     'NOT EMPLOYED', 'UNEMPLOYED', 'RETIRED', 'SELF EMPLOYED',
                                                     'SELF-EMPLOYED', 'NONE', 'N/A', 'HOMEMAKER', 'STUDENT',
                                                     'INFORMATION REQUESTED', 'REFUND', ''
            ) then true
        else false
        end as is_non_corporate

from {{ source('congress', 'politician_top_donors') }}