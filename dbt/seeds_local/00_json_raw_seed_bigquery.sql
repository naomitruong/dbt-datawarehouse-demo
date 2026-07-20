-- Local-only bootstrap for the `json_raw` landing layer on BigQuery.
--
-- In production these tables are created and filled by the Kafka ingest jobs (Debezium/Mongo
-- topics -> json_raw.*). This script recreates a minimal, self-consistent dataset so the dbt
-- lineage can be run end-to-end on BigQuery. It mirrors the original Postgres seed:
--   * `data` is a BigQuery JSON column (was jsonb),
--   * timestamps inside `data` are epoch-milliseconds numbers (Debezium/Mongo style),
--   * only the columns the dbt models actually read are materialised (data/op/ts_ms/inserted_at),
--     plus topic/database_type for context. Tables are partitioned by DATE(inserted_at),
--     matching the Postgres RANGE(inserted_at) partitioning.
--
-- Usage:
--   bq query --use_legacy_sql=false --project_id=<YOUR_PROJECT> \
--     --location=asia-southeast1 < seeds_local/00_json_raw_seed_bigquery.sql
-- (the DDL below uses an unqualified `json_raw` dataset, so it lands in --project_id.)

CREATE SCHEMA IF NOT EXISTS json_raw
    OPTIONS (location = 'asia-southeast1');

-- ── DDL ───────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE json_raw.ecopay_ecopay_stores (
    data JSON,
    op STRING,
    topic STRING,
    database_type STRING,
    ts_ms TIMESTAMP,
    inserted_at TIMESTAMP
) PARTITION BY DATE(inserted_at);

CREATE OR REPLACE TABLE json_raw.ecopay_ecopay_transactions (
    data JSON,
    op STRING,
    topic STRING,
    database_type STRING,
    ts_ms TIMESTAMP,
    inserted_at TIMESTAMP
) PARTITION BY DATE(inserted_at);

CREATE OR REPLACE TABLE json_raw.dms1_users (
    data JSON,
    op STRING,
    topic STRING,
    database_type STRING,
    ts_ms TIMESTAMP,
    inserted_at TIMESTAMP
) PARTITION BY DATE(inserted_at);


-- ── dms1_users ────────────────────────────────────────────────────────────────
-- Three-level org chain: salesman -> agent -> region head.
--   u001 Nguyen Van A  (salesman, 0901000001) -> managed by u002
--   u002 Tran Thi B    (agent,    0902000002) -> managed by u003
--   u003 Le Van C      (head,     0903000003) -> no manager (top of org)
-- u004 is INACTIVE and must be excluded by dm_dms1_user_manager_phones.
INSERT INTO json_raw.dms1_users (data, op, database_type, ts_ms, inserted_at, topic)
SELECT
    PARSE_JSON(FORMAT(
        '{"_id":{"$oid":"%s"},"__v":0,"name":"%s","phone":"%s","code":"%s","email":"%s","status":"%s","direct_manager":%s,"created_at":%d,"updated_at":%d}',
        u.id, u.name, u.phone, u.code, u.email, u.status,
        IF(u.manager IS NULL, 'null', FORMAT('{"$oid":"%s"}', u.manager)),
        UNIX_MILLIS(TIMESTAMP '2025-01-10 08:00:00+07'),
        UNIX_MILLIS(TIMESTAMP '2026-07-01 09:00:00+07')
    )),
    'c',
    'mongodb',
    TIMESTAMP '2026-07-15 02:00:00+00',
    TIMESTAMP '2026-07-15 02:05:00+00',
    'dwh_debezium.mongo.dms.users'
FROM UNNEST([
    STRUCT('64a1000000000000000000u1' AS id, 'Nguyen Van A' AS name, '0901000001' AS phone, 'NV001' AS code, 'a@finviet.test' AS email, 'ACTIVE'   AS status, '64a1000000000000000000u2' AS manager),
    STRUCT('64a1000000000000000000u2', 'Tran Thi B',   '0902000002', 'NV002', 'b@finviet.test', 'ACTIVE',   '64a1000000000000000000u3'),
    STRUCT('64a1000000000000000000u3', 'Le Van C',     '0903000003', 'NV003', 'c@finviet.test', 'ACTIVE',   CAST(NULL AS STRING)),
    STRUCT('64a1000000000000000000u4', 'Pham Thi D',   '0904000004', 'NV004', 'd@finviet.test', 'INACTIVE', '64a1000000000000000000u2')
]) AS u;


-- ── ecopay_ecopay_stores ──────────────────────────────────────────────────────
-- STORE001 -> MERCH001, salesman 0901000001 (u001)
-- STORE002 -> MERCH002, salesman 0902000002 (u002)
-- One merchant_code per store, as enforced by test_dim_ecopay_stores_one_merchant_per_store.
INSERT INTO json_raw.ecopay_ecopay_stores (data, op, database_type, ts_ms, inserted_at, topic)
SELECT
    PARSE_JSON(FORMAT(
        '{"_id":{"$oid":"%s"},"__v":0,"code":"%s","name":"%s","pure_name":"%s","merchant_code":"%s","business_name":"%s","status":"active","phone":"%s","manager_phone":"%s","sale_info":{"phone":"%s","name":"%s"},"bank_code":"VCB","bank_name":"Vietcombank","payment_type":"qrcode","created_at":%d,"approved_at":%d,"updated_at":%d,"is_default":true,"is_supplier":false,"index":1,"rejected_count":0}',
        s.id, s.code, s.name, s.name, s.merchant_code, s.name, s.store_phone, s.sale_phone, s.sale_phone, s.sale_name,
        UNIX_MILLIS(TIMESTAMP '2025-01-15 10:00:00+07'),
        UNIX_MILLIS(TIMESTAMP '2025-02-01 14:30:00+07'),
        UNIX_MILLIS(TIMESTAMP '2026-07-01 09:00:00+07')
    )),
    'c',
    'mongodb',
    TIMESTAMP '2026-07-15 02:00:00+00',
    TIMESTAMP '2026-07-15 02:05:00+00',
    'dwh_debezium.mongo.finviet_ecopay.ecopay_stores'
FROM UNNEST([
    STRUCT('64b1000000000000000000s1' AS id, 'STORE001' AS code, 'Cua hang Test 1' AS name, 'MERCH001' AS merchant_code, '0281000001' AS store_phone, '0901000001' AS sale_phone, 'Nguyen Van A' AS sale_name),
    STRUCT('64b1000000000000000000s2', 'STORE002', 'Cua hang Test 2', 'MERCH002', '0281000002', '0902000002', 'Tran Thi B')
]) AS s;


-- ── ecopay_ecopay_transactions ────────────────────────────────────────────────
-- merchant_code always matches the store's merchant_code, as required by
-- test_dim_ecopay_transactions_merchant_matches_store.
--
-- The mart keeps only status in (paid_processing, success, paid) AND
-- partner_payment_at >= date_trunc(current_date, month) - 2 months, so:
--   TXN0001..TXN0003 -> land in the mart
--   TXN0004 (failed)     -> filtered out by status
--   TXN0005 (2025-03-01) -> filtered out by the 2-month window
INSERT INTO json_raw.ecopay_ecopay_transactions (data, op, database_type, ts_ms, inserted_at, topic)
SELECT
    PARSE_JSON(FORMAT(
        '{"_id":{"$oid":"%s"},"__v":0,"transid":"%s","partner_transid":"%s","store_code":"%s","merchant_code":"%s","merchant_name":"%s","amount":%d,"initial_amount":%d,"original_amount":%d,"total_fee":%f,"total_user_fee":0,"total_cashback_fee":0,"status":"%s","currency":"VND","payment_channel":"qrcode","payment_service":"ecopay","bank_code":"VCB","partner_payment_at":%d,"paid_at":%d,"created_at":%d,"updated_at":%d,"is_refund":false,"is_reconciled":true,"auto_paid":false}',
        t.id, t.transid, CONCAT(t.transid, '_P'), t.store_code, t.merchant_code, t.merchant_name,
        t.amount, t.amount, t.amount, t.total_fee, t.status,
        UNIX_MILLIS(t.paid_at), UNIX_MILLIS(t.paid_at),
        UNIX_MILLIS(TIMESTAMP_SUB(t.paid_at, INTERVAL 5 MINUTE)), UNIX_MILLIS(t.paid_at)
    )),
    'c',
    'mongodb',
    TIMESTAMP '2026-07-15 02:00:00+00',
    TIMESTAMP '2026-07-15 02:05:00+00',
    'dwh_debezium.mongo.finviet_ecopay.ecopay_transactions'
FROM UNNEST([
    STRUCT('64c1000000000000000000t1' AS id, 'TXN0001' AS transid, 'STORE001' AS store_code, 'MERCH001' AS merchant_code, 'Cua hang Test 1' AS merchant_name, 150000 AS amount, 3000.0 AS total_fee, 'success'         AS status, TIMESTAMP '2026-06-15 10:30:00+07' AS paid_at),
    STRUCT('64c1000000000000000000t2', 'TXN0002', 'STORE001', 'MERCH001', 'Cua hang Test 1', 250000, 5000.0, 'paid',            TIMESTAMP '2026-07-10 15:45:00+07'),
    STRUCT('64c1000000000000000000t3', 'TXN0003', 'STORE002', 'MERCH002', 'Cua hang Test 2',  99000, 1980.0, 'paid_processing', TIMESTAMP '2026-07-14 09:15:00+07'),
    STRUCT('64c1000000000000000000t4', 'TXN0004', 'STORE001', 'MERCH001', 'Cua hang Test 1',  50000, 1000.0, 'failed',          TIMESTAMP '2026-07-12 11:00:00+07'),
    STRUCT('64c1000000000000000000t5', 'TXN0005', 'STORE001', 'MERCH001', 'Cua hang Test 1',  70000, 1400.0, 'success',         TIMESTAMP '2025-03-01 08:00:00+07')
]) AS t;
