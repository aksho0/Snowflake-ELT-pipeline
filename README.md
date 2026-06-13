# GlobalMart Data Pipeline using Snowflake

## Overview

GlobalMart is a Snowflake-based data engineering project that demonstrates an end-to-end data pipeline using the Medallion Architecture (Bronze and Silver layers).

The pipeline ingests data from Amazon S3 in multiple formats:

* JSON (IoT sensor events)
* CSV (Point of Sale transactions)
* Parquet (ERP purchase orders)

The project uses Snowflake features such as:

* Storage Integrations
* External Stages
* File Formats
* Snowpipe
* Streams
* COPY INTO
* Semi-structured Data Processing
* LATERAL FLATTEN
* MERGE Operations

---

## Architecture

```text
Amazon S3
    │
    ▼
Storage Integration
    │
    ▼
External Stage
    │
    ▼
Snowpipe
    │
    ▼
Bronze Layer (RAW)
    │
    ▼
Streams
    │
    ▼
Silver Layer (STAGING)
```

---

## Project Structure

### Database

```text
global_mart_db
```

### Schemas

#### Storage Layer

```text
storage_integration_sc
```

Contains:

* Storage Integration
* External Stage
* File Formats

#### Bronze Layer

```text
raw
```

Stores raw ingested data.

#### Silver Layer

```text
staging
```

Stores transformed and cleansed data.

---

# Data Sources

## 1. JSON Data

### Source

IoT sensor event data.

### Objects

| Object Type          | Name            |
| -------------------- | --------------- |
| File Format          | format_json     |
| Raw Table            | events_raw      |
| Structured Raw Table | iot_events_raw  |
| Snowpipe             | json_pipe_raw   |
| Stream               | stream_json_raw |
| Silver Table         | stg_json_sensor |

### Processing

* Raw JSON loaded into VARIANT column
* JSON attributes extracted into relational columns
* Nested arrays flattened using `LATERAL FLATTEN`
* Metadata fields parsed and transformed
* Stream captures incremental changes
* MERGE keeps Silver table synchronized

---

## 2. CSV Data

### Source

Retail Point-of-Sale transactions.

### Objects

| Object Type  | Name                |
| ------------ | ------------------- |
| File Format  | format_csv          |
| Raw Table    | csv_raw_table       |
| Snowpipe     | csv_pipe_raw        |
| Stream       | stream_csv_raw      |
| Silver Table | stg_csv_transaction |

### Processing

* CSV files loaded into RAW table
* Data cleansing and standardization
* Payment method normalization
* Category standardization
* Transaction timestamp generation
* Stream-based incremental processing
* MERGE operation for CDC handling

---

## 3. Parquet Data

### Source

ERP Purchase Orders.

### Objects

| Object Type | Name               |
| ----------- | ------------------ |
| File Format | format_parquet     |
| Raw Table   | parquet_raw_table  |
| Snowpipe    | parquet_pipe_raw   |
| Stream      | stream_parquet_raw |

### Processing

* Parquet files loaded from S3
* Column mapping using:

```sql
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
```

* Supports efficient columnar ingestion

---

# Snowflake Features Demonstrated

## Storage Integration

Secure connection between Snowflake and Amazon S3.

```sql
CREATE STORAGE INTEGRATION
```

---

## External Stage

Provides access to files stored in S3.

```sql
CREATE STAGE
```

---

## Snowpipe

Automated continuous ingestion of files arriving in S3.

```sql
CREATE PIPE
```

---

## Streams

Tracks inserts, updates, and deletes for incremental processing.

```sql
CREATE STREAM
```

---

## Semi-Structured Data Processing

Used for handling JSON data through:

* VARIANT
* FLATTEN
* JSON path expressions

Example:

```sql
raw_payload:metadata:firmware
```

---

## MERGE Operations

Used for CDC-style synchronization between Bronze and Silver layers.

Supports:

* INSERT
* UPDATE
* DELETE

---

# Medallion Architecture

## Bronze Layer (RAW)

Purpose:

* Store source data with minimal transformation
* Maintain source fidelity
* Enable auditing and replay

Tables:

* events_raw
* iot_events_raw
* csv_raw_table
* parquet_raw_table

---

## Silver Layer (STAGING)

Purpose:

* Data cleansing
* Standardization
* Business transformations
* Incremental processing

Tables:

* stg_json_sensor
* stg_csv_transaction

---

# Technologies Used

* Snowflake
* Amazon S3
* Snowpipe
* Snowflake Streams
* SQL
* Semi-Structured Data Processing
* Medallion Architecture

---

# Learning Outcomes

This project demonstrates:

* Cloud data ingestion from S3
* Automated loading with Snowpipe
* Change Data Capture using Streams
* JSON processing with VARIANT and FLATTEN
* Incremental ELT pipelines
* Medallion Architecture implementation
* Data transformation and standardization in Snowflake

---

# Future Enhancements

* Add Gold Layer for business reporting
* Implement Snowflake Tasks for full automation
* Add data quality checks
* Create analytical dashboards using Power BI or Tableau
* Implement monitoring and alerting
* Add data lineage and governance controls
