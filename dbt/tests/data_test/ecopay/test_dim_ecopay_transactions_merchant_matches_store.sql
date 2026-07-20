-- Transactions with store_code must use the store's merchant_code from dim_ecopay_stores.
-- Enforces: one store -> one merchant; transactions must not assign the wrong merchant for a store.

select
    t.transid,
    t.merchant_code as transaction_merchant_code,
    s.code as store_code,
    s.merchant_code as store_merchant_code,
    'transaction_merchant_mismatch_store' as violation
from {{ ref('dim_ecopay_transactions') }} t
inner join {{ ref('dim_ecopay_stores') }} s
    on t.store_code = s.code
where t.store_code is not null
  and s.merchant_code is not null
  and t.merchant_code is distinct from s.merchant_code
