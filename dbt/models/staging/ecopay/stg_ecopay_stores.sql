{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['ecopay', 'staging', 'stores']
) }}

{# Filter data BEFORE expensive JSON processing for better performance #}
WITH filtered_source AS (
    SELECT *
    FROM {{ source('json_raw', 'ecopay_ecopay_stores') }}
    {% if is_incremental() %}
    WHERE inserted_at > (SELECT COALESCE(MAX(json_inserted_at), TIMESTAMP '1970-01-01 00:00:00') FROM {{ this }})
    {% endif %}
)

SELECT
{# Basic identification fields #}
{{ extract_json('data', '_id.$oid') }} AS id,
{{ extract_json('data', '__v') }} AS __v,
{{ extract_json_timestamp_no_timezone('data', 'approved_at') }} AS approved_at,
{{ extract_json('data', 'bank_account_name') }} AS bank_account_name,
{{ extract_json('data', 'bank_account_number') }} AS bank_account_number,
{{ extract_json('data', 'bank_code') }} AS bank_code,
{{ extract_json('data', 'bank_integrated') }} AS bank_integrated,
{{ extract_json('data', 'bank_name') }} AS bank_name,
{{ extract_json('data', 'branch_name') }} AS branch_name,
{{ extract_json('data', 'business_name') }} AS business_name,
{{ extract_json('data', 'category_code') }} AS category_code,
{{ extract_json('data', 'code') }} AS code,
{{ extract_json('data', 'contract_codes', 'JSONB') }} AS contract_codes,
{{ extract_json_timestamp_no_timezone('data', 'created_at') }} AS created_at,
{{ extract_json('data', 'created_by.$oid') }} AS created_by,
{{ extract_json('data', 'crm_org_id') }} AS crm_org_id,
{{ extract_json('data', 'eco_wallet_holder') }} AS eco_wallet_holder,
{{ extract_json('data', 'ecom_config', 'JSONB') }} AS ecom_config,
{{ extract_json('data', 'ewallet_account') }} AS ewallet_account,
{{ extract_json('data', 'expires_payment_link_time') }} AS expires_payment_link_time,
{{ extract_json('data', 'histories', 'JSONB') }} AS histories,
{{ extract_json('data', 'index', 'INT') }} AS index,
{{ extract_json('data', 'integrated_partner', 'JSONB') }} AS integrated_partner,
{{ extract_json('data', 'ipn_url') }} AS ipn_url,
{{ extract_json('data', 'is_allowed_create_payment_link', 'BOOL') }} AS is_allowed_create_payment_link,
{{ extract_json('data', 'is_default', 'BOOL') }} AS is_default,
{{ extract_json('data', 'is_supplier', 'BOOL') }} AS is_supplier,
{{ extract_json('data', 'location', 'JSONB') }} AS location,
{{ extract_json('data', 'location_v2', 'JSONB') }} AS location_v2,
{{ extract_json('data', 'manager_phone') }} AS manager_phone,
{{ extract_json('data', 'manager_team_id.$oid') }} as manager_team_id,
{{ extract_json_timestamp_no_timezone('data', 'mapping_at') }} as mapping_at,
{{ extract_json('data', 'merchant_code') }} AS merchant_code,
{{ extract_json('data', 'my_qrcode_config', 'JSONB') }} AS my_qrcode_config,
{{ extract_json('data', 'name') }} AS name,
{{ extract_json('data', 'note') }} AS note,
{{ extract_json('data', 'notification_channel_config', 'JSONB') }} AS notification_channel_config,
{{ extract_json('data', 'notification_config', 'JSONB') }} AS notification_config,
{{ extract_json('data', 'payment_link_config', 'JSONB') }} AS payment_link_config,
{{ extract_json('data', 'payment_methods', 'JSONB') }} as payment_methods,
{{ extract_json('data', 'payment_type') }} AS payment_type,
{{ extract_json('data', 'phone') }} AS phone,
{{ extract_json('data', 'pos_config', 'JSONB') }} AS pos_config,
{{ extract_json('data', 'pure_name') }} pure_name,
{{ extract_json('data', 'qrcode') }} AS qrcode,
{{ extract_json('data', 'qrcode_list', 'JSONB') }} AS qrcode_list,
{{ extract_json('data', 'qrcode_mapping_code') }} AS qrcode_mapping_code,
{{ extract_json('data', 'qrcode_mapping_code_list', 'JSONB') }} AS qrcode_mapping_code_list,
{{ extract_json('data', 'reasons', 'JSONB') }} AS reasons,
{{ extract_json('data', 'reconcile_method', 'JSONB') }} AS reconcile_method,
{{ extract_json('data', 'refund_ipn_url') }} AS refund_ipn_url,
{{ extract_json_timestamp_no_timezone('data', 'rejected_at') }} AS rejected_at,
{{ extract_json('data', 'rejected_count', 'INT') }} AS rejected_count,
{{ extract_json('data', 'representative_user_info', 'JSONB') }} AS representative_user_info,
{{ extract_json('data', 'request_create_from') }} AS request_create_from,
{{ extract_json('data', 'sale_info', 'JSONB') }} AS sale_info,
{{ extract_json('data', 'secret_key') }} AS secret_key,
{{ extract_json('data', 'softPOS_config', 'JSONB') }} AS softPOS_config,
{{ extract_json('data', 'status') }} AS status,
{{ extract_json('data', 'steps', 'JSONB') }} AS steps,
{{ extract_json('data', 'tax_code') }} AS tax_code,
{{ extract_json('data', 'terminal_name') }} AS terminal_name,
{{ extract_json('data', 'transaction_config', 'JSONB') }} AS transaction_config,
{{ extract_json_timestamp_no_timezone('data', 'updated_at') }} AS updated_at,
{{ extract_json('data', 'updated_by.$oid') }} AS updated_by,
{{ extract_json_timestamp_no_timezone('data', 'updatedAt') }} AS updatedAt,
{{ extract_json('data', 'user_fee_configs', 'JSONB') }} AS user_fee_configs,
{{ extract_json('data', 'zalo', 'JSONB') }} AS zalo,
-- Metadata fields
op,
ts_ms,
inserted_at AS json_inserted_at,
CURRENT_TIMESTAMP() AS inserted_at
FROM filtered_source
