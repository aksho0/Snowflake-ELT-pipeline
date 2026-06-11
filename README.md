# GlobalMart Retail Data Pipeline using Snowflake

## Overview

This project demonstrates an end-to-end data ingestion and transformation pipeline built on Snowflake using the Medallion Architecture (Bronze and Silver Layers).

The pipeline ingests both JSON IoT sensor data and CSV Point-of-Sale (POS) transaction data from AWS S3 into Snowflake. Data is automatically loaded using Snowpipe, tracked through Streams, and transformed into analytics-ready tables in the Silver layer.

---

## Architecture

```text
AWS S3
   │
   ▼
Storage Integration
   │
   ▼
External Stage
   │
   ├── JSON Files
   └── CSV Files
   │
   ▼
Snowpipe (Auto Ingestion)
   │
   ▼
Bronze Layer (RAW)
   │
   ├── iot_events_raw
   └── csv_raw_table
   │
   ▼
Streams
   │
   ▼
Silver Layer (STAGING)
   │
   ├── stg_json_sensor
   └── stg_csv_transaction
```

---

## Technology Stack

* Snowflake
* AWS S3
* Snowpipe
* Snowflake Streams
* Storage Integration
* External Stages
* JSON Processing
* LATERAL FLATTEN
* SQL

---

## Database Structure

### Database

```sql
global_mart_db
```

### Schemas

| Schema                 | Purpose                                   |
| ---------------------- | ----------------------------------------- |
| storage_integration_sc | Storage Integration, Stages, File Formats |
| raw                    | Bronze Layer                              |
| staging                | Silver Layer                              |

---

## Bronze Layer (Raw Data)

### JSON Pipeline

#### File Format

```sql
format_json
```

Features:

* JSON parsing
* `STRIP_OUTER_ARRAY = TRUE`

#### Raw Tables

##### events_raw

Stores complete JSON objects in VARIANT format.

```sql
raw_col VARIANT
```

##### iot_events_raw

Stores parsed IoT event data.

Columns include:

* event_id
* event_type
* store_id
* store_name
* events_ts
* device_id
* raw_payload
* source_file
* loaded_at

---

### JSON Snowpipe

```sql
json_pipe_raw
```

Automatically loads newly arrived JSON files from S3.

---

### JSON Stream

```sql
stream_json_raw
```

Captures incremental changes from:

```sql
iot_events_raw
```

---

## CSV Pipeline

### File Format

```sql
format_csv
```

Configuration:

* Comma Delimited
* Header Skip
* Null Handling
* Date Formatting
* Timestamp Formatting

---

### Raw Table

```sql
csv_raw_table
```

Stores POS transaction records.

Key columns:

* transaction_id
* store_id
* store_name
* customer_id
* product_name
* quantity
* unit_price
* payment_method
* loyalty_points

---

### CSV Snowpipe

```sql
csv_pipe_raw
```

Automatically loads incoming CSV files.

---

### CSV Stream

```sql
stream_csv_raw
```

Tracks incremental data changes.

---

## Silver Layer (Staging)

The Silver Layer contains cleaned and transformed data.

---

### stg_json_sensor

Processes IoT sensor readings from JSON payloads.

Additional extracted fields:

* firmware
* battery_pct
* signal_rssi
* store_floor_sensor
* sensor
* sensor_value
* sensor_unit
* processing_ts

---

### JSON Transformations

#### LATERAL FLATTEN

Used to explode sensor readings array:

```sql
LATERAL FLATTEN(
    INPUT => raw_payload:readings
)
```

#### Metadata Extraction

Extracts:

```sql
firmware
battery_pct
signal_rssi
store_floor
```

from nested JSON structures.

---

### stg_csv_transaction

Stores cleaned POS transaction data.

Additional columns:

```sql
transaction_ts
line_total
processing_time
```

---

### CSV Transformations

#### Payment Method Standardization

| Original    | Standardized |
| ----------- | ------------ |
| Credit Card | CC           |
| Debit Card  | DC           |
| Cash        | C            |

#### Data Quality Checks

* Negative quantities replaced with 0
* Negative unit prices replaced with 0
* Negative discounts replaced with 0

#### Category Standardization

```sql
UPPER(category)
```

#### Timestamp Creation

Combines:

```sql
transaction_date
transaction_time
```

into:

```sql
transaction_ts
```

---

## Change Data Capture (CDC)

This project uses Snowflake Streams for CDC.

Benefits:

* Incremental processing
* Reduced compute costs
* Near real-time data movement
* Efficient MERGE operations

---

## Data Flow

### JSON Data Flow

```text
JSON File
   ↓
S3 Bucket
   ↓
Snowpipe
   ↓
iot_events_raw
   ↓
stream_json_raw
   ↓
LATERAL FLATTEN
   ↓
stg_json_sensor
```

---

### CSV Data Flow

```text
CSV File
   ↓
S3 Bucket
   ↓
Snowpipe
   ↓
csv_raw_table
   ↓
stream_csv_raw
   ↓
Data Validation
   ↓
stg_csv_transaction
```

---

## Key Snowflake Concepts Demonstrated

* Storage Integration
* External Stage
* File Formats
* COPY INTO
* Snowpipe
* Streams
* MERGE Statements
* VARIANT Data Type
* JSON Parsing
* LATERAL FLATTEN
* Incremental Processing
* Medallion Architecture

---

## Learning Outcomes

By completing this project, you will understand:

1. Snowflake Storage Integration with AWS S3
2. Automated ingestion using Snowpipe
3. Working with JSON and CSV data
4. Change Data Capture using Streams
5. Data transformation using MERGE
6. Flattening nested JSON structures
7. Building Bronze and Silver data layers
8. Creating scalable cloud data pipelines

---

## Future Enhancements

* Add Gold Layer for business reporting
* Implement Tasks for automated transformations
* Create Data Quality Monitoring
* Build Snowflake Dashboards
* Integrate with Power BI/Tableau
* Implement Error Logging Framework
* Add Slowly Changing Dimensions (SCD)

---

## Author

**Akshita**

Snowflake Data Engineering Project
GlobalMart Retail Analytics Pipeline
