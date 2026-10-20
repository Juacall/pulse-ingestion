-- stg_members.sql
--
-- Clean and rename raw_members.
{{ config(materialized = 'view') }}

with state_lookup (state_code, state_name, state_fips) as (
    values
        ('AL', 'ALABAMA', '01'),
        ('AK', 'ALASKA', '02'),
        ('AZ', 'ARIZONA', '04'),
        ('AR', 'ARKANSAS', '05'),
        ('CA', 'CALIFORNIA', '06'),
        ('CO', 'COLORADO', '08'),
        ('CT', 'CONNECTICUT', '09'),
        ('DE', 'DELAWARE', '10'),
        ('DC', 'DISTRICT OF COLUMBIA', '11'),
        ('FL', 'FLORIDA', '12'),
        ('GA', 'GEORGIA', '13'),
        ('HI', 'HAWAII', '15'),
        ('ID', 'IDAHO', '16'),
        ('IL', 'ILLINOIS', '17'),
        ('IN', 'INDIANA', '18'),
        ('IA', 'IOWA', '19'),
        ('KS', 'KANSAS', '20'),
        ('KY', 'KENTUCKY', '21'),
        ('LA', 'LOUISIANA', '22'),
        ('ME', 'MAINE', '23'),
        ('MD', 'MARYLAND', '24'),
        ('MA', 'MASSACHUSETTS', '25'),
        ('MI', 'MICHIGAN', '26'),
        ('MN', 'MINNESOTA', '27'),
        ('MS', 'MISSISSIPPI', '28'),
        ('MO', 'MISSOURI', '29'),
        ('MT', 'MONTANA', '30'),
        ('NE', 'NEBRASKA', '31'),
        ('NV', 'NEVADA', '32'),
        ('NH', 'NEW HAMPSHIRE', '33'),
        ('NJ', 'NEW JERSEY', '34'),
        ('NM', 'NEW MEXICO', '35'),
        ('NY', 'NEW YORK', '36'),
        ('NC', 'NORTH CAROLINA', '37'),
        ('ND', 'NORTH DAKOTA', '38'),
        ('OH', 'OHIO', '39'),
        ('OK', 'OKLAHOMA', '40'),
        ('OR', 'OREGON', '41'),
        ('PA', 'PENNSYLVANIA', '42'),
        ('RI', 'RHODE ISLAND', '44'),
        ('SC', 'SOUTH CAROLINA', '45'),
        ('SD', 'SOUTH DAKOTA', '46'),
        ('TN', 'TENNESSEE', '47'),
        ('TX', 'TEXAS', '48'),
        ('UT', 'UTAH', '49'),
        ('VT', 'VERMONT', '50'),
        ('VA', 'VIRGINIA', '51'),
        ('WA', 'WASHINGTON', '53'),
        ('WV', 'WEST VIRGINIA', '54'),
        ('WI', 'WISCONSIN', '55'),
        ('WY', 'WYOMING', '56'),
        ('AS', 'AMERICAN SAMOA', '60'),
        ('GU', 'GUAM', '66'),
        ('MP', 'NORTHERN MARIANA ISLANDS', '69'),
        ('PR', 'PUERTO RICO', '72'),
        ('VI', 'VIRGIN ISLANDS', '78')
),

members as (
    select
        bioguide_id,
        name,
        state,
        chamber,
        district,
        party,
        geoid_cd
    from {{ source('congress', 'raw_members') }}
    where
        bioguide_id is not null
        and trim(bioguide_id) != ''
        and state is not null
)

select
    members.bioguide_id                       as member_id,
    trim(members.name)                        as member_name,
    state_lookup.state_code                   as state_code,
    coalesce(state_lookup.state_name, upper(trim(members.state)))
                                                as state_name,
    state_lookup.state_fips                   as state_fips,
    trim(members.chamber)                     as chamber,
    cast(members.district as integer)         as district_number,
    trim(members.party)                       as party_name,
    members.geoid_cd                          as geoid_cd
from members
left join state_lookup
    on upper(trim(cast(members.state as varchar))) in (
        state_lookup.state_code,
        state_lookup.state_name,
        state_lookup.state_fips
    )
