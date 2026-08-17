/*
DATABASE --> global_mart_db
SCHEMA --> storage_integration_sc
STORAGE INTEGRATION --> s3_integration
STAGE --> s3_stage

Bronze layer :
    SCHEMA --> raw

Silver layer :
    SCHEMA --> staging

JSON -
    Bronze layer : 
        FILE FORMAT --> format_json 
        TABLE 1 (with VARIANT col) --> events_raw
        TABLE 2 (with all coloumns) --> iot_events_raw
        SNOWPIPE --> json_pipe raw
        STREAM --> stream_json_raw
    Silver layer :
        TABLE --> stg_json_sensor

CSV -
    Bronze layer :
        FILE FORMAT --> format_csv
        TABLE --> csv_raw_table
        SNOWPIPE --> csv_pipe_raw
        STREAM --> stream_csv_raw
    Silver layer :
        TABLE --> stg_csv_transaction

PARQUET -
    Bronze layer :
        FILE FORMAT --> format_parquet
        TABLE --> parquet_raw_table
        SNOWPIPE --> parquet_pipe_raw
        STREAM --> stream_parquet_raw

*/

CREATE DATABASE IF NOT EXISTS global_mart_db
    COMMENT = 'GlobalMart retail Platforms';

CREATE SCHEMA IF NOT EXISTS global_mart_db.storage_integration_sc
    COMMENT = 'Storage Integration, File Formats.';

DESCRIBE DATABASE global_mart_db;


USE global_mart_db.storage_integration_sc;

-- CREATE OR REPLACE STORAGE INTEGRATION s3_integration
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = 'S3'
--     ENABLED = TRUE
--     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::583900470875:role/global-mart-role'
--     STORAGE_ALLOWED_LOCATIONS = ('s3://global-mart-project/');

-- DESC INTEGRATION s3_integration;


-- CREATE OR REPLACE STAGE global_mart_db.storage_integration_sc.s3_stage
--     URL = 's3://global-mart-project/'
--     STORAGE_INTEGRATION = s3_integration;
    

LIST @global_mart_db.storage_integration_sc.s3_stage;


-- *************************************************************** --
-- ************************ BRONZE LAYER ************************* --
-- *************************************************************** --

CREATE SCHEMA IF NOT EXISTS global_mart_db.raw;

-- #########################################
-- ****************** JSON *****************
-- #########################################


-- File format is for the JSON file
CREATE OR REPLACE FILE FORMAT global_mart_db.storage_integration_sc.format_json -- why didn't we use raw schema here
  TYPE = 'JSON'
  strip_outer_array = TRUE;  -- files are arrays of objects:[{...},{....}] => {}
  
DESC FILE FORMAT global_mart_db.storage_integration_sc.format_json;


-- Table for JSON raw data
CREATE TABLE IF NOT EXISTS global_mart_db.raw.events_raw(
  raw_col  variant);
  

-- Manually inserting data in events_raw table from S3 bucket
COPY INTO global_mart_db.raw.events_raw
    FROM @global_mart_db.storage_integration_sc.s3_stage
    FILES = ('iot_events_batch_01.json')
    FILE_FORMAT = (FORMAT_NAME='global_mart_db.storage_integration_sc.format_json');

SELECT * FROM Global_mart_db.raw.events_raw;


-- Table for json file data (for table format)
CREATE OR REPLACE TABLE global_mart_db.raw.iot_events_raw (
    event_id STRING,
    event_type STRING,
    store_id STRING,
    store_name STRING,
    events_ts TIMESTAMP,
    device_id STRING,
    raw_payload VARIANT,

    source_file STRING,
    loaded_at TIMESTAMP
);


-- Manully coping json file's data into table
COPY INTO global_mart_db.raw.iot_events_raw 
    FROM (
        SELECT
        $1:event_id::STRING,
        $1:event_type::STRING,
        $1:store_id::STRING,
        $1:store_name::STRING,
        
        $1:timestamp::TIMESTAMP as events_ts,     
        $1:device_id::STRING,
        $1 as raw_payload,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP()

        FROM @global_mart_db.storage_integration_sc.s3_stage/iot_events_batch_01
    )
    FILE_FORMAT = (FORMAT_NAME='global_mart_db.storage_integration_sc.format_json');

SELECT * FROM global_mart_db.raw.iot_events_raw;


-- Crating snowpipe for auto updatation for iot_events_raw
CREATE OR REPLACE PIPE global_mart_db.raw.json_pipe_raw
    AUTO_INGEST = TRUE
    AS
    COPY INTO global_mart_db.raw.iot_events_raw 
    FROM (
        SELECT
        $1:event_id::STRING,
        $1:event_type::STRING,
        $1:store_id::STRING,
        $1:store_name::STRING,
        
        $1:timestamp::TIMESTAMP as events_ts,     
        $1:device_id::STRING,
        $1 as raw_payload,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP()

        FROM @global_mart_db.storage_integration_sc.s3_stage/iot_events_batch_01
    )
    FILE_FORMAT = (FORMAT_NAME='global_mart_db.storage_integration_sc.format_json');

DESC PIPE global_mart_db.raw.json_pipe_raw;


-- Creating stream for JSON 
CREATE OR REPLACE STREAM global_mart_db.raw.stream_json_raw
 ON TABLE global_mart_db.raw.iot_events_raw
 APPEND_ONLY = TRUE;

SELECT * FROM global_mart_db.raw.stream_json_raw;


-- #######################################
-- **************** CSV ******************
-- #######################################

-- File Format for CSV
CREATE OR REPLACE FILE FORMAT global_mart_db.storage_integration_sc.format_csv 
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    TRIM_SPACE = TRUE
    NULL_IF = ('NULL', 'null', '', 'N/A')
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS';


-- Table for CSV raw data
CREATE TABLE IF NOT EXISTS global_mart_db.raw.csv_raw_table(
  transaction_id string,
  store_id string,
  store_name string,
  store_city string,
  store_region string,
  cashier_id string,
  customer_id string,
  transaction_date date,
  transaction_time time,
  product_sku string,
  product_name string,
  category string,
  subcategory string,
  quantity int,	
  unit_price decimal,
  discount_pct int,
  total_amount decimal,
  payment_method string,
  loyalty_points int
);


-- Manually copying CSV data from S3 bucket into csv_raw_table
COPY INTO global_mart_db.raw.csv_raw_table
    FROM  @global_mart_db.storage_integration_sc.s3_stage
    FILES = ('pos_batch_jan_apr - pos_batch_jan_apr.csv')
    FILE_FORMAT = (FORMAT_NAME = 'global_mart_db.storage_integration_sc.format_csv');

SELECT * FROM global_mart_db.raw.csv_raw_table;


-- Crating snowpipe for auto updatation in csv_raw_table
CREATE OR REPLACE PIPE global_mart_db.raw.csv_pipe_raw 
AUTO_INGEST = TRUE
AS
COPY INTO global_mart_db.raw.csv_raw_table
    FROM  @global_mart_db.storage_integration_sc.s3_stage/pos_batch_jan_apr-pos_batch_jan_apr.csv
    FILE_FORMAT = (FORMAT_NAME = 'global_mart_db.storage_integration_sc.format_csv');

DESC PIPE global_mart_db.raw.csv_pipe_raw;


-- Creating stream for CSV
CREATE OR REPLACE STREAM global_mart_db.raw.stream_csv_raw 
    ON TABLE global_mart_db.raw.csv_raw_table
    APPEND_ONLY = TRUE;

SELECT * FROM global_mart_db.raw.stream_csv_raw;


-- #######################################
-- ************* PARQUET *****************
-- #######################################


-- File Format for parquet
CREATE OR REPLACE FILE FORMAT global_mart_db.storage_integration_sc.format_parquet
    TYPE = 'parquet';

-- Raw table for parquet file
CREATE OR REPLACE TABLE global_mart_db.raw.parquet_raw_table (
    order_id VARCHAR,
    order_date VARCHAR,
    store_id VARCHAR,
    store_city VARCHAR,
    supplier_id VARCHAR,
    supplier_name VARCHAR,
    supplier_city VARCHAR,
    product_sku VARCHAR,
    category VARCHAR,
    quantity_ordered BIGINT,
    quantity_received BIGINT,
    unit_cost DOUBLE,
    total_cost DOUBLE,
    order_status VARCHAR,
    expected_delivery VARCHAR,
    actual_delivery VARCHAR,
    warehouse_id VARCHAR,
    lead_time_days BIGINT,
    is_late BOOLEAN
);


-- Manually copying parquet data from S3 bucket into parquet_raw_table
COPY INTO global_mart_db.raw.parquet_raw_table 
    FROM @global_mart_db.storage_integration_sc.s3_stage
    FILES = ('erp_orders.parquet')
    FILE_FORMAT = (FORMAT_NAME = 'global_mart_db.storage_integration_sc.format_parquet')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT * from global_mart_db.raw.parquet_raw_table;


-- Crating snowpipe for auto updatation in parquet_raw_table
CREATE OR REPLACE PIPE global_mart_db.raw.parquet_pipe_raw
AUTO_INGEST = TRUE
AS
COPY INTO global_mart_db.raw.parquet_raw_table 
    FROM @global_mart_db.storage_integration_sc.s3_stage/erp_orders.parquet
    FILE_FORMAT = (FORMAT_NAME = 'global_mart_db.storage_integration_sc.format_parquet')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


-- Creating stream for parquet
CREATE OR REPLACE STREAM global_mart_db.raw.stream_parquet_raw
    ON TABLE global_mart_db.raw.parquet_raw_table
    APPEND_ONLY = TRUE;

SELECT * FROM global_mart_db.raw.stream_parquet_raw;



-- *************************************************************** --
-- ************************ SILVER LAYER ************************* --
-- *************************************************************** --


CREATE SCHEMA IF NOT EXISTS staging;

-- ##################################
-- ************ JSON ****************
-- ##################################

-- Table for JSON data transformation
CREATE OR REPLACE TABLE global_mart_db.staging.stg_json_sensor (
    event_id string,
    event_type string,
    store_id string,
    store_name string,
    events_ts TIMESTAMP,
    device_id string,

    -- new coloumns for variant data
    firmware VARCHAR,
    battery_pct INT,
    signal_rssi INT,
    store_floor_sensor INT,
    sensor VARCHAR,
    sensor_value float ,
    sensor_unit varchar,
    
    source_file string,
    loaded_at TIMESTAMP,
    processing_ts TIMESTAMP
);

SELECT * FROM global_mart_db.staging.stg_json_sensor;


-- Data transfirmation and insertion
INSERT INTO global_mart_db.staging.stg_json_sensor (
    SELECT
        event_id,
        event_type,
        store_id,
        store_name,
        events_ts,
        device_id,
        raw_payload:metadata:firmware::VARCHAR AS firmware,
        raw_payload:metadata:battery_pct::INT AS battery_pct,
        raw_payload:metadata:signal_rssi::INT AS signal_rssi,
        raw_payload:metadata:store_floor::INT AS store_floor_sensor,
        f.value:sensor::VARCHAR AS sensor,
        f.value:value::VARCHAR AS sensor_value,
        f.value:unit::VARCHAR AS sensor_unit,
        source_file,
        loaded_at,
        CURRENT_TIMESTAMP() AS processing_ts,
    FROM global_mart_db.raw.iot_events_raw,
    LATERAL FLATTEN(INPUT => raw_payload:readings) f
);

SELECT * FROM global_mart_db.staging.stg_json_sensor;

-- Merging stream with table 
MERGE INTO global_mart_db.staging.stg_json_sensor t
USING (
    SELECT
        event_id,
        event_type,
        store_id,
        store_name,
        events_ts,
        device_id,
        raw_payload:metadata:firmware::VARCHAR AS firmware,
        raw_payload:metadata:battery_pct::INT AS battery_pct,
        raw_payload:metadata:signal_rssi::INT AS signal_rssi,
        raw_payload:metadata:store_floor::INT AS store_floor_sensor,
        f.value:sensor::VARCHAR AS sensor,
        f.value:value::VARCHAR AS sensor_value,
        f.value:unit::VARCHAR AS sensor_unit,
        source_file,
        loaded_at,
        CURRENT_TIMESTAMP() AS processing_ts,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM global_mart_db.raw.stream_json_raw,
    LATERAL FLATTEN(INPUT => raw_payload:readings) f
) s
ON t.event_id = s.event_id

WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' AND s.METADATA$ISUPDATE = 'TRUE'
    THEN UPDATE SET
        t.event_type = s.event_type,
        t.store_id = s.store_id,
        t.store_name = s.store_name,
        t.events_ts = s.events_ts,
        t.device_id = s.device_id,
        t.firmware = s.firmware,
        t.battery_pct = s.battery_pct,
        t.signal_rssi = s.signal_rssi,
        t.store_floor_sensor = s.store_floor_sensor,
        t.sensor = s.sensor,
        t.sensor_value = s.sensor_value,
        t.sensor_unit = s.sensor_unit,
        t.source_file = s.source_file,
        t.loaded_at = s.loaded_at,
        t.processing_ts = s.processing_ts

WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' AND s.METADATA$ISUPDATE = 'FALSE'
    THEN INSERT (
        event_id,
        event_type,
        store_id,
        store_name,
        events_ts,
        device_id,
        firmware,
        battery_pct,
        signal_rssi,
        store_floor_sensor,
        sensor,
        sensor_value,
        sensor_unit,
        source_file,
        loaded_at,
        processing_ts
    ) VALUES (
            s.event_id,
            s.event_type,
            s.store_id,
            s.store_name,
            s.events_ts,
            s.device_id,
            s.firmware,
            s.battery_pct,
            s.signal_rssi,
            s.store_floor_sensor,
            s.sensor,
            s.sensor_value,
            s.sensor_unit,
            s.source_file,
            s.loaded_at,
            s.processing_ts
        )

WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' AND s.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE;


-- ##################################
-- ************* CSV ****************
-- ##################################

-- Table for CSV data transformation
CREATE OR REPLACE TABLE global_mart_db.staging.stg_csv_transaction (
    transaction_id string,
    store_id string,
    store_name string,
    store_city string,
    store_region string,
    cashier_id string,
    customer_id string,
    transaction_date date,
    transaction_time time,
    product_sku string,
    product_name string,
    category string,
    subcategory string,
    quantity int,	
    unit_price decimal,
    discount_pct int,
    total_amount decimal,
    payment_method string,
    loyalty_points int,

    transaction_ts timestamp, -- for date+time 
    line_total float,         
    processing_time timestamp 
);

SELECT * FROM global_mart_db.staging.stg_csv_transaction;


-- Data transformatoin and insertion
INSERT INTO global_mart_db.staging.stg_csv_transaction
(
    SELECT
        transaction_id,
        store_id,
        store_name,
        store_city,
        store_region,
        cashier_id,
        customer_id,
        transaction_date,
        transaction_time,
        product_sku,
        product_name,
        UPPER(category) AS category,
        subcategory,
        CASE 
            WHEN quantity > 0 THEN quantity
            ELSE 0
        END AS quantity,
        CASE 
            WHEN unit_price > 0 THEN unit_price
            ELSE 0
        END AS unit_price,
        CASE 
            WHEN discount_pct >= 0 THEN discount_pct
            ELSE 0
        END AS discount_pct,
        total_amount,
        CASE
            WHEN UPPER(payment_method) = 'CREDIT CARD' THEN 'CC'
            WHEN UPPER(payment_method) = 'DEBIT CARD' THEN 'DC'
            ELSE payment_method
        END AS payment_method,
        loyalty_points,
        TO_TIMESTAMP(CONCAT(transaction_date::VARCHAR, ' ', transaction_time::VARCHAR)) AS transaction_ts,
        0 AS line_total,
        CURRENT_TIMESTAMP() AS processing_time,
    FROM global_mart_db.raw.csv_raw_table
);

SELECT * FROM  global_mart_db.staging.stg_csv_transaction;


-- Mergeing stream into table 
MERGE INTO global_mart_db.staging.stg_csv_transaction s 
USING (
    SELECT
        transaction_id,
        store_id,
        store_name,
        store_city,
        store_region,
        cashier_id,
        customer_id,
        transaction_date,
        transaction_time,
        product_sku,
        product_name,
        UPPER(category) AS category,
        subcategory,
        CASE 
            WHEN quantity > 0 THEN quantity
            ELSE 0
        END AS quantity,
        CASE 
            WHEN unit_price > 0 THEN unit_price
            ELSE 0
        END AS unit_price,
        CASE 
            WHEN discount_pct >= 0 THEN discount_pct
            ELSE 0
        END AS discount_pct,
        total_amount,
        CASE
            WHEN UPPER(payment_method) = 'CREDIT CARD' THEN 'CC'
            WHEN UPPER(payment_method) = 'DEBIT CARD' THEN 'DC'
            ELSE payment_method
        END AS payment_method,
        loyalty_points,
        TO_TIMESTAMP(CONCAT(transaction_date::VARCHAR, ' ', transaction_time::VARCHAR)) AS transaction_ts,
        0 AS line_total,
        CURRENT_TIMESTAMP() AS processing_time,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM global_mart_db.raw.stream_csv_raw ) st
ON s.transaction_id = st.transaction_id

WHEN MATCHED AND st.METADATA$ACTION = 'INSERT' AND st.METADATA$ISUPDATE = TRUE
    THEN UPDATE SET
        s.store_id = st.store_id,
        s.store_name = st.store_name,
        s.store_city = st.store_city,
        s.store_region = st.store_region,
        s.cashier_id = st.cashier_id,
        s.customer_id = st.customer_id,
        s.transaction_date = st.transaction_date,
        s.transaction_time = st.transaction_time,
        s.transaction_ts = st.transaction_ts,
        s.product_sku = st.product_sku,
        s.product_name = st.product_name,
        s.category = st.category,
        s.subcategory = st.subcategory,
        s.quantity = st.quantity,
        s.unit_price = st.unit_price,
        s.discount_pct = st.discount_pct,
        s.total_amount = st.total_amount,
        s.line_total = st.line_total,
        s.payment_method = st.payment_method,
        s.loyalty_points = st.loyalty_points,
        s.processing_time = st.processing_time

WHEN NOT MATCHED AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = FALSE
    THEN INSERT (
        transaction_id,
        store_id,
        store_name,
        store_city,
        store_region,
        cashier_id,
        customer_id,
        transaction_date,
        transaction_time,
        transaction_ts,
        product_sku,
        product_name,
        category,
        subcategory,
        quantity,
        unit_price,
        discount_pct,
        total_amount,
        
        payment_method,
        loyalty_points,
        line_total,
        processing_time
        ) VALUES (
            st.transaction_id,
            st.store_id,
            st.store_name,
            st.store_city,
            st.store_region,
            st.cashier_id,
            st.customer_id,
            st.transaction_date,
            st.transaction_time,
            st.transaction_ts,
            st.product_sku,
            st.product_name,
            st.category,
            st.subcategory,
            st.quantity,
            st.unit_price,
            st.discount_pct,
            st.total_amount,
            st.payment_method,
            st.loyalty_points,
            st.line_total,
            st.processing_time
        )   
WHEN MATCHED AND st.METADATA$ACTION = 'DELETE' AND st.METADATA$ISUPDATE = FALSE
    THEN DELETE;



-- ##################################
-- ************ PARQUET *************
-- ##################################

CREATE OR REPLACE TABLE global_mart_db.staging.stg_erp_orders (
    order_id VARCHAR,
    order_date DATE,
    store_id VARCHAR,
    store_city VARCHAR,
    supplier_id VARCHAR,
    supplier_name VARCHAR,
    supplier_city VARCHAR,
    product_sku VARCHAR,
    category VARCHAR,
    quantity_ordered BIGINT,
    quantity_received BIGINT,
    unit_cost NUMBER(12,2),
    total_cost NUMBER(14,2),
    order_status VARCHAR,
    expected_delivery DATE,
    actual_delivery DATE,
    warehouse_id VARCHAR,
    lead_time_days BIGINT,
    is_late BOOLEAN,
    processing_time TIMESTAMP
);

-- inserting values
INSERT INTO global_mart_db.staging.stg_erp_orders
SELECT
    order_id,
    TO_DATE(order_date),
    store_id,
    store_city,
    supplier_id,
    supplier_name,
    supplier_city,
    product_sku,
    UPPER(category),
    quantity_ordered,
    quantity_received,
    unit_cost,
    total_cost,
    UPPER(order_status),
    TO_DATE(expected_delivery),
    TO_DATE(actual_delivery),
    warehouse_id,
    lead_time_days,
    is_late,
    CURRENT_TIMESTAMP()
FROM global_mart_db.raw.parquet_raw_table;

-- verifying 
SELECT COUNT(*)
FROM global_mart_db.staging.stg_erp_orders; --exprected 45000



-- *************************************************************** --
-- ************************* GOLD LAYER ************************** --
-- *************************************************************** --

CREATE SCHEMA IF NOT EXISTS marts;

-- ============================================================
-- 1. DIMENSION: DATE
-- ============================================================

CREATE OR REPLACE TABLE global_mart_db.marts.dim_date (
    date_key        NUMBER(8,0),
    full_date       DATE,
    day_of_month    NUMBER(2,0),
    day_name        VARCHAR,
    week_of_year    NUMBER(2,0),
    month_number    NUMBER(2,0),
    month_name      VARCHAR,
    quarter_number  NUMBER(1,0),
    year_number     NUMBER(4,0)
);

INSERT INTO global_mart_db.marts.dim_date
SELECT DISTINCT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_key,
    full_date,
    DAY(full_date) AS day_of_month,
    DAYNAME(full_date) AS day_name,
    WEEKOFYEAR(full_date) AS week_of_year,
    MONTH(full_date) AS month_number,
    MONTHNAME(full_date) AS month_name,
    QUARTER(full_date) AS quarter_number,
    YEAR(full_date) AS year_number
FROM (
    SELECT transaction_date AS full_date
    FROM global_mart_db.staging.stg_csv_transaction
    WHERE transaction_date IS NOT NULL

    UNION

    SELECT order_date AS full_date
    FROM global_mart_db.staging.stg_erp_orders
    WHERE order_date IS NOT NULL

    UNION

    SELECT TO_DATE(events_ts) AS full_date
    FROM global_mart_db.staging.stg_json_sensor
    WHERE events_ts IS NOT NULL
);

SELECT * FROM global_mart_db.marts.dim_date;

-- ============================================================
-- 2. DIMENSION: STORE
-- ============================================================

CREATE OR REPLACE TABLE global_mart_db.marts.dim_store (
    store_key          NUMBER AUTOINCREMENT,
    store_id           VARCHAR,
    store_name         VARCHAR,
    store_city         VARCHAR,
    store_region       VARCHAR,
    store_floor_sensor NUMBER,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO global_mart_db.marts.dim_store (
    store_id,
    store_name,
    store_city,
    store_region,
    store_floor_sensor
)
SELECT
    store_id,
    store_name,
    store_city,
    store_region,
    store_floor_sensor
FROM (
    SELECT
        store_id,
        store_name,
        store_city,
        store_region,
        NULL AS store_floor_sensor
    FROM global_mart_db.staging.stg_csv_transaction
    WHERE store_id IS NOT NULL

    UNION

    SELECT
        store_id,
        store_name,
        NULL AS store_city,
        NULL AS store_region,
        store_floor_sensor
    FROM global_mart_db.staging.stg_json_sensor
    WHERE store_id IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY store_id
    ORDER BY store_name
) = 1;

SELECT * FROM global_mart_db.marts.dim_store;

-- ============================================================
-- 3. DIMENSION: PRODUCT
-- ============================================================

CREATE OR REPLACE TABLE global_mart_db.marts.dim_product (
    product_key   NUMBER AUTOINCREMENT,
    product_sku   VARCHAR,
    product_name  VARCHAR,
    category      VARCHAR,
    subcategory   VARCHAR,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO global_mart_db.marts.dim_product (
    product_sku,
    product_name,
    category,
    subcategory
)
SELECT
    product_sku,
    product_name,
    category,
    subcategory
FROM global_mart_db.staging.stg_csv_transaction
WHERE product_sku IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_sku
    ORDER BY product_name
) = 1;

SELECT * FROM global_mart_db.marts.dim_product;


-- ============================================================
-- 4. DIMENSION: CUSTOMER
-- ============================================================

CREATE OR REPLACE TABLE global_mart_db.marts.dim_customer (
    customer_key NUMBER AUTOINCREMENT,
    customer_id  VARCHAR,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO global_mart_db.marts.dim_customer (
    customer_id
)
SELECT DISTINCT
    customer_id
FROM global_mart_db.staging.stg_csv_transaction
WHERE customer_id IS NOT NULL;


SELECT * FROM global_mart_db.marts.dim_customer;


-- ============================================================
-- 5. DIMENSION: SUPPLIER
-- ============================================================

CREATE OR REPLACE TABLE global_mart_db.marts.dim_supplier (
    supplier_key  NUMBER AUTOINCREMENT,
    supplier_id   VARCHAR,
    supplier_name VARCHAR,
    supplier_city VARCHAR,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO global_mart_db.marts.dim_supplier (
    supplier_id,
    supplier_name,
    supplier_city
)
SELECT
    supplier_id,
    supplier_name,
    supplier_city
FROM global_mart_db.staging.stg_erp_orders
WHERE supplier_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY supplier_id
    ORDER BY supplier_name
) = 1;

SELECT * FROM global_mart_db.marts.dim_supplier;


-- ============================================================
-- ============================================================
-- FACT TABLES
-- ============================================================
-- ============================================================


-- ============================================================
-- 6. FACT: SALES
-- Grain:
-- One row per POS transaction
-- ============================================================

CREATE OR REPLACE TABLE global_mart_db.marts.fct_sales (
    sales_key          NUMBER AUTOINCREMENT,

    transaction_id     VARCHAR,
    date_key           NUMBER(8,0),

    store_key          NUMBER,
    product_key        NUMBER,
    customer_key       NUMBER,

    transaction_date   DATE,
    transaction_ts     TIMESTAMP,

    quantity           NUMBER,
    unit_price         NUMBER(18,2),
    discount_pct       NUMBER(10,2),

    gross_amount       NUMBER(18,2),
    discount_amount    NUMBER(18,2),
    net_sales_amount   NUMBER(18,2),

    payment_method     VARCHAR,
    loyalty_points     NUMBER,

    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO global_mart_db.marts.fct_sales (
    transaction_id,
    date_key,
    store_key,
    product_key,
    customer_key,
    transaction_date,
    transaction_ts,
    quantity,
    unit_price,
    discount_pct,
    gross_amount,
    discount_amount,
    net_sales_amount,
    payment_method,
    loyalty_points
)
SELECT
    s.transaction_id,

    TO_NUMBER(
        TO_CHAR(s.transaction_date, 'YYYYMMDD')
    ) AS date_key,

    ds.store_key,
    dp.product_key,
    dc.customer_key,

    s.transaction_date,
    s.transaction_ts,

    s.quantity,
    s.unit_price,
    s.discount_pct,

    s.quantity * s.unit_price AS gross_amount,

    (s.quantity * s.unit_price)
        * (s.discount_pct / 100.0) AS discount_amount,

    s.total_amount AS net_sales_amount,

    s.payment_method,
    s.loyalty_points

FROM global_mart_db.staging.stg_csv_transaction s

LEFT JOIN global_mart_db.marts.dim_store ds
    ON s.store_id = ds.store_id

LEFT JOIN global_mart_db.marts.dim_product dp
    ON s.product_sku = dp.product_sku

LEFT JOIN global_mart_db.marts.dim_customer dc
    ON s.customer_id = dc.customer_id;

SELECT * FROM global_mart_db.marts.fct_sales;

-- ============================================================
-- 7. FACT: DAILY SALES
--
-- Required by the GlobalMart project outline.
--
-- Grain:
-- One row per date + store + category
--
-- Metrics:
-- Revenue
-- Units
-- Transactions
-- Customers
-- Average basket size
-- ============================================================

CREATE OR REPLACE TABLE global_mart_db.marts.fct_daily_sales (
    sales_date          DATE,
    store_key           NUMBER,
    category            VARCHAR,

    total_revenue       NUMBER(18,2),
    total_units         NUMBER,
    transaction_count   NUMBER,
    customer_count      NUMBER,

    avg_basket_size     NUMBER(18,2),

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO global_mart_db.marts.fct_daily_sales (
    sales_date,
    store_key,
    category,
    total_revenue,
    total_units,
    transaction_count,
    customer_count,
    avg_basket_size
)
SELECT
    s.transaction_date AS sales_date,

    ds.store_key,

    s.category,

    SUM(s.total_amount) AS total_revenue,

    SUM(s.quantity) AS total_units,

    COUNT(DISTINCT s.transaction_id) AS transaction_count,

    COUNT(DISTINCT s.customer_id) AS customer_count,

    SUM(s.total_amount)
        / NULLIF(COUNT(DISTINCT s.transaction_id), 0)
        AS avg_basket_size

FROM global_mart_db.staging.stg_csv_transaction s

JOIN global_mart_db.marts.dim_store ds
    ON s.store_id = ds.store_id

GROUP BY
    s.transaction_date,
    ds.store_key,
    s.category;

SELECT * FROM global_mart_db.marts.fct_daily_sales;


-- ============================================================
-- 8. FACT: PURCHASE ORDERS
--
-- Grain:
-- One row per ERP purchase order
-- ============================================================

CREATE OR REPLACE TABLE global_mart_db.marts.fct_purchase_orders (
    order_key           NUMBER AUTOINCREMENT,

    order_id            VARCHAR,
    date_key            NUMBER(8,0),

    store_key           NUMBER,
    supplier_key        NUMBER,

    order_date          DATE,
    category            VARCHAR,

    product_sku         VARCHAR,

    quantity_ordered    NUMBER,
    quantity_received   NUMBER,

    unit_cost           NUMBER(18,2),
    total_cost          NUMBER(18,2),

    order_status        VARCHAR,

    expected_delivery   DATE,
    actual_delivery     DATE,

    warehouse_id        VARCHAR,
    lead_time_days      NUMBER,

    is_late             BOOLEAN,

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO global_mart_db.marts.fct_purchase_orders (
    order_id,
    date_key,
    store_key,
    supplier_key,
    order_date,
    category,
    product_sku,
    quantity_ordered,
    quantity_received,
    unit_cost,
    total_cost,
    order_status,
    expected_delivery,
    actual_delivery,
    warehouse_id,
    lead_time_days,
    is_late
)
SELECT
    e.order_id,

    TO_NUMBER(
        TO_CHAR(e.order_date, 'YYYYMMDD')
    ) AS date_key,

    ds.store_key,
    sp.supplier_key,

    e.order_date,
    e.category,
    e.product_sku,

    e.quantity_ordered,
    e.quantity_received,

    e.unit_cost,
    e.total_cost,

    e.order_status,

    e.expected_delivery,
    e.actual_delivery,

    e.warehouse_id,
    e.lead_time_days,

    e.is_late

FROM global_mart_db.staging.stg_erp_orders e

LEFT JOIN global_mart_db.marts.dim_store ds
    ON e.store_id = ds.store_id

LEFT JOIN global_mart_db.marts.dim_supplier sp
    ON e.supplier_id = sp.supplier_id;

SELECT * FROM global_mart_db.marts.fct_purchase_orders;


-- ============================================================
-- 9. FACT: STORE CATEGORY MARGIN
--
-- Grain:
-- One row per sales date + store + category
--
-- POS revenue is compared with ERP procurement cost.
--
-- IMPORTANT:
-- ERP product_sku intentionally differs from POS product_sku.
-- Therefore the business join is store_id + category.
-- ============================================================

CREATE OR REPLACE TABLE global_mart_db.marts.fct_store_category_margin (
    margin_date          DATE,
    store_key            NUMBER,
    category             VARCHAR,

    sales_revenue        NUMBER(18,2),
    procurement_cost     NUMBER(18,2),

    gross_margin         NUMBER(18,2),
    gross_margin_pct     NUMBER(10,2),

    sales_units          NUMBER,
    ordered_units        NUMBER,
    received_units       NUMBER,

    late_order_count     NUMBER,

    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO global_mart_db.marts.fct_store_category_margin (
    margin_date,
    store_key,
    category,
    sales_revenue,
    procurement_cost,
    gross_margin,
    gross_margin_pct,
    sales_units,
    ordered_units,
    received_units,
    late_order_count
)

WITH sales AS (

    SELECT
        transaction_date,
        store_id,
        category,

        SUM(total_amount) AS sales_revenue,
        SUM(quantity) AS sales_units

    FROM global_mart_db.staging.stg_csv_transaction

    GROUP BY
        transaction_date,
        store_id,
        category
),

orders AS (

    SELECT
        order_date,
        store_id,
        category,

        SUM(total_cost) AS procurement_cost,
        SUM(quantity_ordered) AS ordered_units,
        SUM(quantity_received) AS received_units,

        SUM(
            CASE
                WHEN is_late = TRUE THEN 1
                ELSE 0
            END
        ) AS late_order_count

    FROM global_mart_db.staging.stg_erp_orders

    GROUP BY
        order_date,
        store_id,
        category
)

SELECT
    s.transaction_date AS margin_date,

    ds.store_key,

    s.category,

    s.sales_revenue,

    COALESCE(o.procurement_cost, 0) AS procurement_cost,

    s.sales_revenue
        - COALESCE(o.procurement_cost, 0)
        AS gross_margin,

    CASE
        WHEN s.sales_revenue = 0 THEN 0
        ELSE
            (
                (
                    s.sales_revenue
                    - COALESCE(o.procurement_cost, 0)
                )
                / s.sales_revenue
            ) * 100
    END AS gross_margin_pct,

    s.sales_units,

    COALESCE(o.ordered_units, 0),
    COALESCE(o.received_units, 0),
    COALESCE(o.late_order_count, 0)

FROM sales s

JOIN global_mart_db.marts.dim_store ds
    ON s.store_id = ds.store_id

LEFT JOIN orders o
    ON s.transaction_date = o.order_date
    AND s.store_id = o.store_id
    AND s.category = o.category;

SELECT * FROM global_mart_db.marts.fct_store_category_margin;


-- ============================================================
-- 10. FACT: IOT SENSOR READINGS
--
-- Grain:
-- One row per sensor reading
--
-- The source contains nested readings[] arrays.
-- Silver has already flattened those readings.
-- ============================================================

CREATE OR REPLACE TABLE global_mart_db.marts.fct_iot_sensor_readings (
    sensor_reading_key NUMBER AUTOINCREMENT,

    event_id            VARCHAR,
    date_key            NUMBER(8,0),

    store_key           NUMBER,

    event_type          VARCHAR,
    device_id           VARCHAR,

    events_ts           TIMESTAMP,

    sensor              VARCHAR,
    sensor_value        NUMBER(18,4),
    sensor_unit         VARCHAR,

    firmware            VARCHAR,
    battery_pct         NUMBER,
    signal_rssi         NUMBER,
    store_floor_sensor  NUMBER,

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO global_mart_db.marts.fct_iot_sensor_readings (
    event_id,
    date_key,
    store_key,
    event_type,
    device_id,
    events_ts,
    sensor,
    sensor_value,
    sensor_unit,
    firmware,
    battery_pct,
    signal_rssi,
    store_floor_sensor
)
SELECT
    i.event_id,

    TO_NUMBER(
        TO_CHAR(TO_DATE(i.events_ts), 'YYYYMMDD')
    ) AS date_key,

    ds.store_key,

    i.event_type,
    i.device_id,
    i.events_ts,

    i.sensor,

    i.sensor_value::NUMBER(18,4) AS sensor_value,

    i.sensor_unit,

    i.firmware,
    i.battery_pct,
    i.signal_rssi,
    i.store_floor_sensor

FROM global_mart_db.staging.stg_json_sensor i

LEFT JOIN global_mart_db.marts.dim_store ds
    ON i.store_id = ds.store_id;

SELECT * FROM global_mart_db.marts.fct_iot_sensor_readings;


-- ============================================================
-- VALIDATION
-- ============================================================

-- Dimension counts
SELECT 'DIM_DATE' AS OBJECT_NAME,
       COUNT(*) AS ROW_COUNT
FROM global_mart_db.marts.dim_date

UNION ALL

SELECT 'DIM_STORE',
       COUNT(*)
FROM global_mart_db.marts.dim_store

UNION ALL

SELECT 'DIM_PRODUCT',
       COUNT(*)
FROM global_mart_db.marts.dim_product

UNION ALL

SELECT 'DIM_CUSTOMER',
       COUNT(*)
FROM global_mart_db.marts.dim_customer

UNION ALL

SELECT 'DIM_SUPPLIER',
       COUNT(*)
FROM global_mart_db.marts.dim_supplier;


-- Fact counts
SELECT 'FCT_SALES' AS OBJECT_NAME,
       COUNT(*) AS ROW_COUNT
FROM global_mart_db.marts.fct_sales

UNION ALL

SELECT 'FCT_DAILY_SALES',
       COUNT(*)
FROM global_mart_db.marts.fct_daily_sales

UNION ALL

SELECT 'FCT_PURCHASE_ORDERS',
       COUNT(*)
FROM global_mart_db.marts.fct_purchase_orders

UNION ALL

SELECT 'FCT_STORE_CATEGORY_MARGIN',
       COUNT(*)
FROM global_mart_db.marts.fct_store_category_margin

UNION ALL

SELECT 'FCT_IOT_SENSOR_READINGS',
       COUNT(*)
FROM global_mart_db.marts.fct_iot_sensor_readings;