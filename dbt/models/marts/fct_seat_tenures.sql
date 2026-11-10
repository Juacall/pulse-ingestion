-- models/marts/fct_seat_tenures.sql
{{ config(materialized='table') }}

WITH base_tenures AS (
    SELECT
        t.politician_id,
        t.chamber,
        t.state_code,
        t.district,
        t.congress,
        t.start_year,
        t.end_year,
        t.start_date,
        t.end_date,
        t.is_active_term,
        -- Assign Senate seat designation (1 or 2) or zero-padded House district
        CASE
            WHEN t.chamber = 'Senate' THEN
                CAST(DENSE_RANK() OVER (
                    PARTITION BY t.state_code, t.chamber
                    ORDER BY t.start_year ASC, t.politician_id ASC
                ) AS TEXT)
            ELSE LPAD(t.district::TEXT, 2, '0')
        END AS seat_identifier
    FROM {{ ref('stg_member_terms') }} t
),

ranked_tenures AS (
    SELECT
        CONCAT(
            b.state_code, '-',
            IF(b.chamber = 'House', 'H', 'S'), '-',
            b.seat_identifier, '-',
            b.congress
        ) AS tenure_id,
        CONCAT(
            b.state_code, '-',
            IF(b.chamber = 'House', 'H', 'S'), '-',
            b.seat_identifier
        ) AS seat_id,
        b.politician_id,
        b.congress,
        b.start_year,
        b.end_year,
        b.start_date,
        b.end_date,
        b.is_active_term,
        ROW_NUMBER() OVER (
            PARTITION BY
                b.state_code,
                b.chamber,
                b.seat_identifier
            ORDER BY b.start_year DESC, b.congress DESC
        ) AS recency_rank
    FROM base_tenures b
)

SELECT
    tenure_id,
    seat_id,
    politician_id,
    congress,
    start_year,
    end_year,
    start_date,
    end_date,
    CASE
        WHEN recency_rank = 1 AND is_active_term = TRUE THEN TRUE
        ELSE FALSE
        END AS is_current_incumbent
FROM ranked_tenures