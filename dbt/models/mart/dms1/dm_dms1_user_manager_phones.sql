{{
    config(
        materialized='table',
        cluster_by=['user_phone'],
        tags=['dms', 'dms-v1', 'mart', 'dms', 'users']
    )
}}

-- each phone number maps to the full phone chain of its user's management hierarchy
-- BigQuery note: cycles are bounded by the level<=5 depth cap (mirrors the Postgres limit);
-- BigQuery recursive CTEs cannot carry a "visited" array through UNNEST, so depth is the guard.
with recursive manager_hierarchy as (
    select
        id as original_user_id,
        direct_manager as manager_id,
        1 as level
    from {{ ref('dim_dms1_users') }}
    where direct_manager is not null
      and status = 'ACTIVE'

    union all

    -- walk up one level at a time until reaching top of org or hitting the depth limit
    select
        mh.original_user_id,
        u.direct_manager,
        mh.level + 1
    from {{ ref('dim_dms1_users') }} u
    inner join manager_hierarchy mh on u.id = mh.manager_id
    where u.direct_manager is not null
      and u.status = 'ACTIVE'
      and mh.level <= 5
),
hierarchy_agg as (
    select
        mh.original_user_id as user_id,
        array_agg(du.phone ignore nulls order by mh.level) as manager_phones,
        array_agg(du.name  ignore nulls order by mh.level) as manager_names
    from manager_hierarchy mh
    left join {{ ref('dim_dms1_users') }} du on du.id = mh.manager_id
    group by mh.original_user_id
)
{% if is_incremental() %}
, run_watermark as (
    select coalesce(max(inserted_at), TIMESTAMP '1970-01-01') as ts
    from {{ this }}
),
recently_changed_ids as (
    select id from {{ ref('dim_dms1_users') }}
    where inserted_at > (select ts from run_watermark)
),
affected_user_ids as (
    -- when a manager's info changes, all their subordinates must be recalculated too
    select id as user_id
    from {{ ref('dim_dms1_users') }}
    where id in (select id from recently_changed_ids)

    union all

    select original_user_id as user_id
    from manager_hierarchy
    where manager_id in (select id from recently_changed_ids)
)
{% endif %}
select
    u.phone as user_phone,
    min(u.name) as user_name,
    array_agg(distinct hp ignore nulls) as hierarchy_phone,  -- all phones in the chain: user + managers
    min(ha.manager_phones[safe_offset(0)]) as agent_phone,   -- direct manager
    min(ha.manager_names[safe_offset(0)]) as agent_name,
    max(u.inserted_at) as source_inserted_at,
    CURRENT_TIMESTAMP() as inserted_at
from {{ ref('dim_dms1_users') }} u
left join hierarchy_agg ha on u.id = ha.user_id
cross join unnest(
    array_concat([u.phone], ifnull(ha.manager_phones, cast([] as array<string>)))
) as hp
where u.status = 'ACTIVE'
  and hp is not null
{% if is_incremental() %}
  and u.id in (select user_id from affected_user_ids)
{% endif %}
group by u.phone
