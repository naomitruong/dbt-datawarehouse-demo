-- Each store (by id and by code) belongs to exactly one merchant (merchant_code).
-- Fails when the same store id/code is linked to more than one merchant_code.

select
    'store_id_multiple_merchants' as violation,
    id as store_id,
    code as store_code,
    count(distinct merchant_code) as merchant_count
from {{ ref('dim_ecopay_stores') }}
where merchant_code is not null
group by id, code
having count(distinct merchant_code) > 1

union all

select
    'store_code_multiple_merchants' as violation,
    cast(null as string) as store_id,
    code as store_code,
    count(distinct merchant_code) as merchant_count
from {{ ref('dim_ecopay_stores') }}
where code is not null
  and merchant_code is not null
group by code
having count(distinct merchant_code) > 1
