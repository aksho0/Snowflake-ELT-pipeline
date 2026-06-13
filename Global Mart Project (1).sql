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

DESC PIPE global_mart_db.storage_integration_sc.csv_pipe_raw;


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

