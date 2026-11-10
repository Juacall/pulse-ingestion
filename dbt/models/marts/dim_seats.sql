-- models/marts/dim_seats.sql
{{ config(materialized='table') }}

WITH house_seats AS (
    SELECT DISTINCT
        CONCAT(state_code, '-H-', LPAD(CAST(district_number AS TEXT), 2, '0')) AS seat_id,
        state_code,
        'House' AS chamber,
        LPAD(CAST(district_number AS TEXT), 2, '0') AS district,
        CAST(NULL AS TEXT) AS seat_class
    FROM {{ ref('stg_members') }}
    WHERE chamber = 'House'
      AND state_code IS NOT NULL
      AND district_number IS NOT NULL
),

senate_seats AS (
    SELECT DISTINCT
        CONCAT(state_code, '-S-', seat_num) AS seat_id,
        state_code,
        'Senate' AS chamber,
        '00' AS district,
        seat_num AS seat_class
    FROM {{ ref('stg_members') }}
    CROSS JOIN (VALUES ('1'), ('2')) AS senate_classes(seat_num)
    WHERE chamber = 'Senate'
      AND state_code IS NOT NULL
)

SELECT * FROM house_seats
UNION ALL
SELECT * FROM senate_seats