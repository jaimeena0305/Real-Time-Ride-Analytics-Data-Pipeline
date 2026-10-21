# Uber Real-Time Data Engineering Pipeline

An end-to-end real-time data engineering project that simulates Uber ride-booking events and processes them using Microsoft Azure, Azure Event Hubs, Azure Databricks, PySpark, and Delta Lake.

The pipeline demonstrates real-time event ingestion, batch and streaming data processing, data transformation, and analytical data modelling using a Medallion Architecture.

The goal is to build a scalable data pipeline that converts raw ride-booking events into structured datasets suitable for downstream analytics.

## Project Overview

Ride-booking platforms generate continuous streams of data, including booking details, passenger information, driver information, vehicle attributes, payment methods, ride status, and fare calculations.

This project simulates that data flow and processes ride events through multiple data engineering stages.

The pipeline includes:

- Synthetic ride data generation using Python and Faker.
- Real-time event publishing through Azure Event Hubs.
- Streaming ingestion into Azure Databricks.
- Batch and streaming data processing.
- Bronze and Silver layer data organization.
- Data enrichment using lookup datasets.
- Gold-layer dimensional modelling.
- Fact and dimension tables for analytical workloads.
- Slowly Changing Dimensions (SCD) for tracking changes in selected dimension tables.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Ride data generation and event publishing |
| Faker | Generating synthetic ride-booking data |
| Azure Event Hubs | Real-time event ingestion |
| Apache Kafka Protocol | Connecting Databricks to Event Hubs |
| Azure Databricks | Cloud-based data processing |
| PySpark | Distributed data transformation |
| Spark Structured Streaming | Processing streaming ride events |
| Delta Lake | Reliable storage for processed datasets |
| Lakeflow Declarative Pipelines / Spark Declarative Pipelines | Defining streaming tables and data processing flows |
| SQL | Data transformation and enrichment |
| FastAPI | Supporting the Python application's API layer |

**Note:** Azure Data Factory is not listed as an implemented component because its integration is not established by the supplied project code.

## Architecture

The project follows a Medallion Architecture with separate stages for ingestion, transformation, and analytical modelling.

```text
                 SYNTHETIC RIDE DATA
                         |
                         v
                  Python + Faker
                         |
                         v
                  Azure Event Hubs
                         |
                         v
               Kafka-Compatible Ingestion
                         |
                         v
                   BRONZE LAYER
                    rides_raw
                         |
                         v
                   SILVER LAYER
                    stg_rides
                         |
                         v
                 DATA ENRICHMENT
                silver_obt / OBT
                         |
                         v
                    GOLD LAYER
                         |
              +----------+----------+
              |                     |
              v                     v
       Dimension Tables        Fact Table
              |                     |
              +----------+----------+
                         |
                         v
                ANALYTICS-READY DATA
```

The architecture represents the intended data flow. Exact storage paths, catalog names, and pipeline configuration depend on the Databricks environment.

## Data Generation

The project uses Python and Faker to generate synthetic ride-booking records.

Generated attributes include:

- Ride ID and confirmation number.
- Passenger and driver identifiers.
- Vehicle identifiers and attributes.
- Pickup and drop-off locations.
- Booking, pickup, and drop-off timestamps.
- Ride distance and duration.
- Base fare, distance fare, and time fare.
- Surge multiplier, subtotal, tips, and total fare.
- Payment method and ride status.
- Driver rating and cancellation information.

Additional mapping datasets provide descriptive information for cities, vehicle types, vehicle manufacturers, payment methods, ride statuses, and cancellation reasons.

The generated records are published to Azure Event Hubs for streaming ingestion.

**Dataset note:** The records are synthetic and intended for learning and demonstration, not actual Uber operational data.

## Data Pipeline

### 1. Event Generation and Ingestion

**Technologies:** Python, Faker, Azure Event Hubs

The Python application generates ride-booking events and publishes them to Azure Event Hubs.

The Event Hubs service exposes a Kafka-compatible endpoint that allows Azure Databricks to consume the incoming events.

Key operations:

- Generate realistic ride-booking records.
- Serialize ride records into JSON.
- Publish events to an Event Hub.
- Make events available to downstream streaming consumers.

Relevant files:

- `data.py`
- `connection.py`
- `api.py`

### 2. Bronze Layer — Raw Event Ingestion

**Technologies:** Azure Databricks, Kafka-compatible ingestion, PySpark

The Bronze layer ingests streaming events from Azure Event Hubs.

The `ingest.py` pipeline reads events using Spark's Kafka source and converts the message payload into a string column named `rides`.

The raw streaming table is named:

`rides_raw`

Key operations:

- Read events from the configured Event Hub.
- Consume messages using the Kafka protocol.
- Convert event payloads into strings.
- Expose raw ride events through a streaming table.
- Configure streaming offsets and ingestion limits.

Relevant file:

`Code_Files/ingest.py`

### 3. Silver Layer — Structured Data Processing

**Technologies:** PySpark, Spark Declarative Pipelines

The Silver processing stage converts raw event payloads into structured ride records.

The pipeline defines a schema for ride attributes and parses JSON messages into typed columns.

It supports both initial bulk loading and streaming ingestion through separate flows into the staging table.

The staging table is named:

`stg_rides`

Key operations:

- Parse JSON event payloads.
- Apply a predefined schema.
- Convert fields into appropriate data types.
- Process historical and streaming records.
- Prepare structured data for downstream transformations.

Relevant file:

`Code_Files/silver.py`

### 4. Data Enrichment and One Big Table

**Technologies:** SQL, PySpark, Streaming Tables

The enrichment stage combines ride records with descriptive mapping datasets.

Examples include:

- Vehicle manufacturers.
- Vehicle types and pricing attributes.
- Payment methods.
- Ride statuses.
- Cities and geographical information.
- Cancellation reasons.

The resulting enriched dataset is represented by the streaming table:

`silver_obt`

OBT stands for One Big Table.

This stage makes descriptive attributes available alongside ride-level information, simplifying downstream analytical modelling.

Relevant files:

- `Code_Files/silver_obt.sql`
- `Code_Files/silver_obt.ipynb`

### 5. Gold Layer — Dimensional Data Modelling

**Technologies:** PySpark, Declarative Pipelines, Delta Lake

The Gold layer organizes enriched ride data into analytical fact and dimension tables.

The supplied modelling code defines the following entities:

| Table | Purpose |
|---|---|
| `dim_passenger` | Passenger attributes |
| `dim_driver` | Driver attributes and ratings |
| `dim_vehicle` | Vehicle attributes |
| `dim_payment` | Payment method information |
| `dim_booking` | Booking and ride details |
| `dim_location` | Location attributes and historical changes |
| `fact` | Ride-level metrics and foreign-key identifiers |

The fact table includes measures and attributes such as:

- Distance travelled.
- Ride duration.
- Base fare.
- Distance fare.
- Time fare.
- Surge multiplier.
- Total fare.
- Tip amount.
- Ride rating.

The dimension tables provide descriptive context for analytical queries.

#### Slowly Changing Dimensions

The modelling code defines automated change data capture flows using `create_auto_cdc_flow`.

It configures SCD Type 1 for selected dimensions and SCD Type 2 for the location dimension.

- **SCD Type 1:** Maintains the latest attribute values without retaining previous versions.
- **SCD Type 2:** Supports historical versions of dimension records.

The location dimension uses `city_updated_at` as its sequencing field to support historical change processing.

Relevant file:

`Code_Files/model.py`

**Implementation note:** The table definitions and CDC configurations require a compatible Databricks pipeline environment. The complete pipeline must be executed and validated before claiming that all tables and historical tracking work successfully.

## Repository Structure

```text
Uber_Data_Engineer_Project/
│
├── README.md
├── architecture.png
├── Uber_Project.svg
│
├── api.py
├── connection.py
├── data.py
├── files_array.json
│
├── pyproject.toml
├── requirements.txt
├── .gitignore
│
├── Code_Files/
│   ├── ingest.py
│   ├── silver.py
│   ├── model.py
│   ├── silver_obt.sql
│   ├── silver_obt.ipynb
│   ├── bronze_adls.ipynb
│   └── readme.md
│
├── Data/
│   ├── bulk_rides.json
│   ├── map_cancellation_reasons.json
│   ├── map_cities.json
│   ├── map_payment_methods.json
│   ├── map_ride_statuses.json
│   ├── map_vehicle_makes.json
│   └── map_vehicle_types.json
│
└── templates/
    ├── confirmation.html
    └── home.html
```

## Prerequisites

Before running the pipeline, configure the following:

1. An Azure subscription.
2. An Azure Event Hubs namespace and Event Hub.
3. An Azure Databricks workspace with compatible compute.
4. Appropriate Event Hubs credentials and permissions.
5. A compatible Declarative Pipelines environment.
6. Storage locations and catalog/schema configuration for the required tables.
7. Python 3.12 or another version compatible with the project's dependencies.

The exact runtime requirements depend on the selected Databricks environment.

## Configuration

### Azure Event Hubs

Create an Event Hub and configure the producer application using environment variables.

Create a local `.env` file:

```env
CONNECTION_STRING=<your-event-hubs-connection-string>
EVENT_HUBNAME=<your-event-hub-name>
```

Never commit credentials, access keys, or connection strings to GitHub.

In Databricks, configure the Event Hubs connection string through an appropriate secret scope or other secure configuration mechanism.

The streaming notebook and Python producer must reference the correct Event Hub and namespace.

### Databricks Configuration

Review the pipeline files and configure:

- Catalog and schema names.
- Bronze and Silver table locations.
- Source dataset paths.
- Event Hubs connection settings.
- Streaming pipeline dependencies.
- Required permissions and compute resources.

The supplied code contains environment-specific names that may need to be adjusted before execution.

## Running the Project

### Step 1: Clone the Repository

```bash
git clone <your-github-repository-url>
cd Uber_Data_Engineer_Project
```

### Step 2: Install Python Dependencies

Create and activate a virtual environment, then install the project dependencies.

```bash
python -m venv .venv
```

Activate the environment using the appropriate command for your operating system.

Install dependencies using the package manager supported by the repository.

### Step 3: Configure Event Hubs

Create the Event Hub, configure the environment variables, and verify that the producer has permission to publish messages.

### Step 4: Generate Ride Events

Run the Python application using its configured entry point to generate ride records and publish them to Event Hubs.

For a simple producer test, review `connection.py`.

### Step 5: Configure Databricks

Import the relevant notebooks and configure the streaming source, catalog, schemas, and storage locations.

Ensure that the environment supports the pipeline APIs used by the project.

### Step 6: Run the Streaming Pipeline

Execute the ingestion and transformation stages in dependency order:

1. Ingest Event Hubs messages into `rides_raw`.
2. Parse and stage events in `stg_rides`.
3. Enrich the records to produce `silver_obt`.
4. Build the Gold-layer dimension and fact tables.

Configure any required initial bulk-load flows before processing the complete dataset.

### Step 7: Validate the Output

Verify that:

- New ride events reach the ingestion layer.
- JSON messages are parsed into structured records.
- Historical and streaming data are handled as expected.
- Lookup information is correctly joined.
- Fact and dimension tables contain valid records.
- CDC processing handles updates as intended.
- The pipeline can recover from expected operational failures.

## Example Analytical Queries

The following SQL examples illustrate the types of analysis that can be performed after the relevant Gold tables have been created and validated.

### 1. Total Revenue by City

```sql
SELECT
    l.city,
    SUM(f.total_fare) AS total_revenue
FROM fact f
JOIN dim_location l
    ON f.pickup_city_id = l.pickup_city_id
GROUP BY l.city
ORDER BY total_revenue DESC;
```

This query requires the Gold tables to be in the same accessible schema and the location join to match the implemented model.

### 2. Average Ride Duration

```sql
SELECT
    AVG(duration_minutes) AS average_ride_duration
FROM fact;
```

### 3. Average Fare by Payment Method

```sql
SELECT
    p.payment_method,
    AVG(f.total_fare) AS average_fare
FROM fact f
JOIN dim_payment p
    ON f.payment_method_id = p.payment_method_id
GROUP BY p.payment_method
ORDER BY average_fare DESC;
```

These queries should be adapted to the actual catalog, schema, column names, and dimension relationships used by the deployed pipeline.

## Engineering Concepts Demonstrated

This project provides practical exposure to:

- Real-time event-driven data ingestion.
- Kafka-compatible streaming integrations.
- Python-based synthetic data generation.
- Batch and streaming processing.
- PySpark transformations.
- JSON parsing and schema definition.
- Medallion Architecture.
- Data enrichment and lookup joins.
- Fact and dimension modelling.
- Slowly Changing Dimensions.
- Change data capture.
- Cloud data pipeline configuration.

## Future Enhancements

Potential improvements include:

- Implementing automated data quality checks.
- Adding structured error handling and failed-record quarantine.
- Configuring checkpointing and recovery for streaming workloads.
- Handling duplicate and late-arriving events.
- Implementing event-time processing and watermarking where appropriate.
- Improving idempotency and replay handling.
- Adding automated pipeline monitoring and alerting.
- Implementing incremental processing benchmarks.
- Adding unit tests for transformations and schema validation.
- Building a dashboard over the Gold-layer tables.
- Documenting deployment steps and infrastructure configuration.

## Disclaimer

This project is intended for educational and portfolio purposes.

It uses synthetic ride-booking data and does not represent actual Uber production systems.

Successful execution depends on Azure resources, Databricks runtime compatibility, credentials, table configuration, and pipeline dependencies.

The repository includes pipeline definitions and supporting code; the complete deployment should be tested before describing it as a fully operational real-time data engineering system.

---

**Project Focus:** Real-Time Data Engineering | Azure Event Hubs | Databricks | PySpark | Structured Streaming | Delta Lake | Dimensional Modelling
