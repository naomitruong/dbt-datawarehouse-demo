{{
    config(
        materialized='incremental',
        unique_key=['transid'],
        incremental_strategy='merge',
        cluster_by=['merchant_code', 'salesman_phone'],
        tags=["ecopay", "mart","dms", "transaction"]
    )
}}

{% if is_incremental() %}
with user_watermark as (
    select coalesce(max(mart.dim_users_ts), TIMESTAMP '1970-01-01') as ts
    from {{ this }} mart
),
store_watermark as (
    select coalesce(max(mart.dim_stores_ts), TIMESTAMP '1970-01-01') as ts
    from {{ this }} mart
),
source_watermark as (
    select coalesce(max(mart.source_inserted_at), TIMESTAMP '1970-01-01') as ts
    from {{ this }} mart
),
recently_changed_users as (
    select user_phone from {{ ref('dm_dms1_user_manager_phones') }}
    where inserted_at > (select ts from user_watermark)
),
updated_store_codes as (
    select code from {{ ref('dim_ecopay_stores') }}
    where inserted_at > (select ts from store_watermark)
),
affected_transids as (
    -- salesman/manager changed or deactivated
    select mart.transid from {{ this }} mart
    inner join recently_changed_users rcu on rcu.user_phone = mart.salesman_phone

    union distinct

    -- store salesman reassigned
    select mart.transid from {{ this }} mart
    inner join updated_store_codes usc on usc.code = mart.merchant_code
)
{% endif %}
select
    et.transid,
    et.partner_payment_at as transaction_date,
    ump.agent_name as agent_name,
    ump.user_name as salesman_name,
    ump.hierarchy_phone,
    JSON_VALUE(es.sale_info, '$.phone') as salesman_phone,
    ump.agent_phone,
    et.merchant_code as merchant_transaction_code,
    es.name as merchant_name,
    es.code as merchant_code,
    es.created_at as merchant_created_date,
    es.approved_at as merchant_onboarding_date,
    et.amount as gmv,
    0 as commission,
    et.total_fee as services_fee,
    et.inserted_at  as source_inserted_at,
    ump.inserted_at as dim_users_ts,
    es.inserted_at  as dim_stores_ts,
    CURRENT_TIMESTAMP() as inserted_at
from {{ ref('dim_ecopay_transactions') }} et
left join {{ ref('dim_ecopay_stores') }} es on et.store_code = es.code
left join {{ ref('dm_dms1_user_manager_phones') }} ump on ump.user_phone = JSON_VALUE(es.sale_info, '$.phone')
where
et.store_code is not null
and et.status in ('paid_processing', 'success', 'paid')

{% if is_incremental() %}
    and (
        -- existing mart rows needing update (hierarchy/merchant/store changed): any date, mart-scoped
        et.transid in (select transid from affected_transids)
        -- new/updated transactions from source: scoped to 2-month window to prevent pulling historical backfills
        or (
            et.partner_payment_at >= TIMESTAMP(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 MONTH))
            and et.inserted_at > (select ts from source_watermark)
        )
    )
{% else %}
    -- full refresh: load last 2 months only
    and et.partner_payment_at >= TIMESTAMP(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 MONTH))
{% endif %}
