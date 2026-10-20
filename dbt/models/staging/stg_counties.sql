-- models/staging/stg_counties.sql

select distinct
    lpad(cast(GEOID_COUNTY_20 as varchar), 5, '0') as county_fips,
    trim(NAMELSAD_COUNTY_20)                      as county_name
from {{ ref('raw_census__cd11920_county20') }}
where GEOID_COUNTY_20 is not null