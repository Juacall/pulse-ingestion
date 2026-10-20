-- models/rag/view_rag_documents.sql

{{ config(materialized = 'view') }}

select
    -- Unified surrogate key or unified activity ID
    la.legislative_activity_sk as doc_id,
    coalesce(la.amendment_id, la.bill_id) as activity_id,
    la.member_id,
    m.chamber,
    m.state_code,
    m.party_name,
    m.district_number,
    la.activity_type,
    la.bill_status,
    la.bill_latest_action_date as update_date,

    -- Text payload for vector embeddings
    'Chamber: ' || coalesce(m.chamber, 'N/A') || '\n' ||
    'Legislator: ' || coalesce(m.member_name, 'N/A') || ' (' || coalesce(m.party_name, '') || '-' || coalesce(m.state_code, '') || coalesce(' District ' || cast(m.district_number as varchar), '') || ')\n' ||
    'Relationship: ' || coalesce(la.relationship, 'N/A') || '\n' ||
    'Activity Type: ' || coalesce(la.activity_type, 'N/A') || '\n' ||
    'Activity ID: ' || coalesce(coalesce(la.amendment_id, la.bill_id), 'N/A') || '\n' ||
    'Title: ' || coalesce(la.bill_title, 'No title provided') || '\n' ||
    'Parent Bill: ' || coalesce(la.bill_title, 'N/A') || '\n' ||
    'Latest Action: ' || coalesce(la.bill_latest_action_text, 'No action text recorded') as page_content

from {{ ref('int_legislative_activity') }} as la
left join {{ ref('stg_members') }} as m
on la.member_id = m.member_id