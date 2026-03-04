-- =============================================================================
--  LAKEFLOW PIPELINE  —  Test Data Setup
--  Run this in a Databricks SQL Notebook or SQL Editor before starting the
--  pipeline.  Creates the Bronze catalog/schema/tables with realistic sample
--  data including intentional duplicates and bad rows so you can see every
--  pipeline feature in action.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 0.  CATALOG & SCHEMA SETUP
-- -----------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS main;

CREATE SCHEMA IF NOT EXISTS main.bronze
  COMMENT 'Raw Bronze layer — source data for Lakeflow pipeline';

CREATE SCHEMA IF NOT EXISTS main.silver
  COMMENT 'Cleaned Silver layer — output of Lakeflow pipeline';

CREATE SCHEMA IF NOT EXISTS main.quarantine
  COMMENT 'Rows that failed DQ expectations';


-- =============================================================================
-- 1.  BRONZE CUSTOMERS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1a.  Create table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.bronze.customers (
  cust_id    STRING  COMMENT 'Raw customer ID (will be renamed to customer_id)',
  email      STRING  COMMENT 'Customer email address',
  age        STRING  COMMENT 'Age stored as string in Bronze (cast to INT in Silver)',
  country    STRING  COMMENT 'Country — mixed case, needs UPPER()',
  updated_at TIMESTAMP COMMENT 'Last updated timestamp — used as SEQUENCE BY for dedup'
)
USING DELTA
COMMENT 'Bronze raw customers table'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- -----------------------------------------------------------------------------
-- 1b.  Insert sample data
--
--  Scenario A — Duplicates (same cust_id, different updated_at):
--    C001 appears 3 times → pipeline should keep the 2024-06-01 record
--    C003 appears 2 times → pipeline should keep the 2024-05-20 record
--
--  Scenario B — Bad rows (DQ failures):
--    NULL cust_id       → expect_or_drop: valid_customer_id
--    NULL email         → expect_or_drop: valid_email
--
--  Scenario C — Needs transformation:
--    age stored as STRING → cast to INT
--    country mixed case  → UPPER()
--    email has spaces    → TRIM()
-- -----------------------------------------------------------------------------
INSERT INTO main.bronze.customers VALUES

  -- ── Normal records ─────────────────────────────────────────────────────────
  ('C001', 'alice@example.com',       '30', 'Canada',        TIMESTAMP '2024-04-01 08:00:00'),
  ('C002', '  bob@example.com  ',     '25', 'usa',           TIMESTAMP '2024-03-15 12:00:00'),
  ('C004', 'diana@example.com',       '40', 'uk',            TIMESTAMP '2024-02-10 09:00:00'),
  ('C005', 'evan@example.com',        '22', 'australia',     TIMESTAMP '2024-01-20 14:00:00'),
  ('C006', 'fiona@example.com',       '35', 'CANADA',        TIMESTAMP '2024-03-01 11:00:00'),
  ('C007', 'george@example.com',      '28', 'Germany',       TIMESTAMP '2024-04-10 16:00:00'),
  ('C008', 'hannah@example.com',      '31', 'France',        TIMESTAMP '2024-05-05 10:00:00'),

  -- ── DUPLICATES: C001 — 3 versions, pipeline must keep latest (2024-06-01) ──
  ('C001', 'alice_v2@example.com',    '31', 'canada',        TIMESTAMP '2024-05-01 08:00:00'),
  ('C001', 'alice_v3@example.com',    '32', 'CANADA',        TIMESTAMP '2024-06-01 08:00:00'),  -- ← WINNER

  -- ── DUPLICATES: C003 — 2 versions, pipeline must keep latest (2024-05-20) ──
  ('C003', 'carol_old@example.com',   '29', 'india',         TIMESTAMP '2024-04-15 07:00:00'),
  ('C003', 'carol@example.com',       '29', 'India',         TIMESTAMP '2024-05-20 07:00:00'),  -- ← WINNER

  -- ── BAD ROWS: NULL primary key → dropped by valid_customer_id expectation ──
  (NULL,   'nokey@example.com',       '20', 'Spain',         TIMESTAMP '2024-03-01 06:00:00'),

  -- ── BAD ROWS: NULL email → dropped by valid_email expectation ────────────
  ('C009', NULL,                      '27', 'Mexico',        TIMESTAMP '2024-04-22 15:00:00'),

  -- ── BAD ROWS: invalid age → warn only (row kept in Silver) ───────────────
  ('C010', 'zara@example.com',        '999','Brazil',        TIMESTAMP '2024-05-10 08:30:00');


-- =============================================================================
-- 2.  BRONZE ORDERS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2a.  Create table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.bronze.orders (
  order_id        STRING  COMMENT 'Order identifier',
  customer_id     STRING  COMMENT 'Reference to customer',
  amount          STRING  COMMENT 'Order amount stored as string (cast to DOUBLE in Silver)',
  status          STRING  COMMENT 'Order status — needs UPPER()',
  order_timestamp STRING  COMMENT 'Order timestamp as string (cast to TIMESTAMP in Silver)'
)
USING DELTA
COMMENT 'Bronze raw orders table'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- -----------------------------------------------------------------------------
-- 2b.  Insert sample data
--
--  Scenario A — Duplicates:
--    O001 appears 2 times → keep latest order_timestamp
--    O005 appears 3 times → keep latest order_timestamp
--
--  Scenario B — Bad rows:
--    NULL order_id  → dropped by valid_order_id expectation
--    amount = -50   → dropped by positive_amount expectation
--    NULL status    → warn only (row kept)
--
--  Scenario C — Needs transformation:
--    amount is STRING → cast to DOUBLE
--    order_timestamp is STRING → cast to TIMESTAMP
--    order_date derived column → to_date(order_timestamp)
--    status mixed case → UPPER()
-- -----------------------------------------------------------------------------
INSERT INTO main.bronze.orders VALUES

  -- ── Normal records ─────────────────────────────────────────────────────────
  ('O001', 'C001', '150.00',  'completed',  '2024-01-10 10:30:00'),
  ('O002', 'C002', '89.99',   'Pending',    '2024-02-14 14:00:00'),
  ('O003', 'C003', '320.50',  'SHIPPED',    '2024-03-05 09:15:00'),
  ('O004', 'C004', '45.00',   'completed',  '2024-03-20 16:45:00'),
  ('O006', 'C006', '210.75',  'Completed',  '2024-04-18 11:00:00'),
  ('O007', 'C007', '99.00',   'pending',    '2024-05-01 13:30:00'),
  ('O008', 'C001', '500.00',  'completed',  '2024-05-12 08:00:00'),
  ('O009', 'C002', '30.00',   'CANCELLED',  '2024-05-25 17:00:00'),
  ('O010', 'C008', '75.50',   'shipped',    '2024-06-01 10:00:00'),

  -- ── DUPLICATES: O001 — 2 versions, keep latest (2024-01-15) ──────────────
  ('O001', 'C001', '175.00',  'completed',  '2024-01-15 10:30:00'),  -- ← WINNER

  -- ── DUPLICATES: O005 — 3 versions, keep latest (2024-04-05) ──────────────
  ('O005', 'C005', '60.00',   'pending',    '2024-04-01 12:00:00'),
  ('O005', 'C005', '60.00',   'shipped',    '2024-04-03 12:00:00'),
  ('O005', 'C005', '60.00',   'completed',  '2024-04-05 12:00:00'),  -- ← WINNER

  -- ── BAD ROWS: NULL order_id → dropped by valid_order_id expectation ───────
  (NULL,   'C003', '200.00',  'completed',  '2024-03-10 08:00:00'),

  -- ── BAD ROWS: negative amount → dropped by positive_amount expectation ────
  ('O011', 'C004', '-50.00',  'refunded',   '2024-04-25 09:00:00'),

  -- ── BAD ROWS: NULL status → warn only (row kept in Silver) ───────────────
  ('O012', 'C005', '120.00',  NULL,         '2024-05-30 14:00:00');


-- =============================================================================
-- 3.  VERIFICATION QUERIES  —  run these after INSERT to confirm data
-- =============================================================================

-- Check Bronze customers (expect 14 rows including dupes + bad rows)
SELECT
  'customers' AS table_name,
  COUNT(*)                                   AS total_rows,
  COUNT(DISTINCT cust_id)                    AS distinct_cust_ids,
  SUM(CASE WHEN cust_id IS NULL THEN 1 END)  AS null_keys,
  SUM(CASE WHEN email   IS NULL THEN 1 END)  AS null_emails
FROM main.bronze.customers;

-- Check Bronze orders (expect 16 rows including dupes + bad rows)
SELECT
  'orders' AS table_name,
  COUNT(*)                                   AS total_rows,
  COUNT(DISTINCT order_id)                   AS distinct_order_ids,
  SUM(CASE WHEN order_id IS NULL THEN 1 END) AS null_keys,
  SUM(CASE WHEN CAST(amount AS DOUBLE) <= 0
           OR amount IS NULL THEN 1 END)     AS bad_amounts
FROM main.bronze.orders;

-- Preview customers (see duplicates clearly)
SELECT cust_id, email, age, country, updated_at
FROM main.bronze.customers
ORDER BY cust_id, updated_at;

-- Preview orders (see duplicates clearly)
SELECT order_id, customer_id, amount, status, order_timestamp
FROM main.bronze.orders
ORDER BY order_id, order_timestamp;


-- =============================================================================
-- 4.  EXPECTED SILVER RESULTS (after pipeline run — for manual validation)
-- =============================================================================

-- After pipeline runs, Silver customers should have:
--   ✅  10 clean rows  (C001–C010 deduped, NULLs dropped)
--   ✅  C001 → alice_v3@example.com (latest of 3 dupes)
--   ✅  C003 → carol@example.com   (latest of 2 dupes)
--   ✅  country always UPPER-CASED
--   ✅  email trimmed (no spaces)
--   ✅  age as INTEGER
--   ✅  C010 kept (age=999 is warn-only)
--   ❌  NULL cust_id row → dropped
--   ❌  NULL email row   → dropped

-- After pipeline runs, Silver orders should have:
--   ✅  11 clean rows  (O001–O012 deduped, NULLs/negatives dropped)
--   ✅  O001 → amount=175.00  (latest of 2 dupes)
--   ✅  O005 → status=COMPLETED (latest of 3 dupes)
--   ✅  status always UPPER-CASED
--   ✅  amount as DOUBLE
--   ✅  order_timestamp as TIMESTAMP
--   ✅  order_date derived column populated
--   ✅  O012 kept (NULL status is warn-only)
--   ❌  NULL order_id row → dropped
--   ❌  negative amount row → dropped

-- Quick Silver validation after pipeline run:
-- SELECT * FROM main.silver.silver_customers ORDER BY customer_id;
-- SELECT * FROM main.silver.silver_orders    ORDER BY order_id;
