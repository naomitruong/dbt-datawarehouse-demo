{{
    config(
        materialized='view',
        tags=['ecopay', 'mart', 'reporting', 'bi']
    )
}}
-- BI reporting aperture over the mart: a thin view (no business logic) that adds
-- transaction_day (DATE) for BI date controls and hides only the technical/audit
-- columns (dim_*_ts, *_inserted_at). Sales-hierarchy phone numbers are kept on
-- purpose: the revenue report needs the agent / salesman contact numbers.
select
    -- keys / grain
    transid,
    transaction_date,
    date(transaction_date)      as transaction_day,   -- BI date dimension
    merchant_created_date,
    merchant_onboarding_date,
    merchant_name,
    merchant_code,
    merchant_transaction_code,
    -- sales hierarchy: names + contact phones (required by the report)
    agent_name,
    agent_phone,
    salesman_name,
    salesman_phone,
    hierarchy_phone,
    gmv,
    services_fee,
    commission

from {{ ref('dwh_ecopay_transaction_detail') }}
