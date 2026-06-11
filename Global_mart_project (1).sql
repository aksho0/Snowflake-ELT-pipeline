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

CSV -
    Bronze layer :
        FILE FORMAT --> format_csv
        TABLE --> csv_raw_table
        SNOWPIPE --> csv_pipe_raw
        STREAM --> stream_csv_raw

*/

CREATE DATABASE IF NOT EXISTS global_mart_db
    COMMENT = 'GlobalMart retail Platforms';

CREATE SCHEMA IF NOT EXISTS global_mart_db.storage_integration_sc
    COMMENT = 'Storage Integration, File Formats.';

DESCRIBE DATABASE global_mart_db;


USE global_mart_db.storage_integration_sc;

CREATE OR REPLACE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::583900470875:role/global-mart-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://global-mart-project/');

DESC INTEGRATION s3_integration; -- I forgot to add STORAGE_AWS_EXTERNAL_ID in my role trust relationship policy 


CREATE OR REPLACE STAGE global_mart_db.storage_integration_sc.s3_stage
    URL = 's3://global-mart-project/'
    STORAGE_INTEGRATION = s3_integration;

LIST @global_mart_db.storage_integration_sc.s3_stage;


-- *************************************************************** --
-- ************************ BRONZE LAYER ************************* --
-- *************************************************************** --

CREATE SCHEMA IF NOT EXISTS global_mart_db.raw;


-- ****************** JSON *****************

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


-- Table for jsin file data (for table format)
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

-- flatern the json file and load the data in it 


-- ******************* CSV **********************

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
CREATE OR REPLACE PIPE global_mart_db.storage_integration_sc.csv_pipe_raw 
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


-- *************************************************************** --
-- ************************ SILVER LAYER ************************* --
-- *************************************************************** --

-- create a schema name as staging, this will store the silver tables 
-- create these tables in staging:
--      - stg_csv_transaction(col will be same as snow_pipe_table and transaction_ts, proccesed_time)
-- insert into this table by taking the data from the stream
-- Transformation : 
-- payment methods : DC for debit card, CC for creadit card, C for cash

--      - stg_json_sensor(all coloumns as iot_events_raw and sensor_name, firmware(string), sensor_unit(varchar), process_ts)

CREATE SCHEMA IF NOT EXISTS staging;

-- ************************ JSON *********************

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
    store_floor_sensor INT,
    sensor VARCHAR,
    sensor_value float ,
    sensor_unit varchar,
    
    source_file string,
    loaded_at TIMESTAMP , 
    processing_time TIMESTAMP
);

SELECT * FROM global_mart_db.staging.stg_json_sensor;


-- ****************** CSV ************************

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




