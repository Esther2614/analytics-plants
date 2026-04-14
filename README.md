# analytics-plants

## Init Project

Define dbt_project.yml and sources.yml

# 📊 Overall Plan for Data Cleaning & dbt Modeling

## Project Overview
This project builds a structured and reliable data pipeline using **dbt** and **Snowflake**.  
The goal is to transform raw data from the `LOAD` database into clean, standardized, and analytics-ready datasets.

The pipeline follows a layered approach:

RAW → BASE → INTERMEDIATE → ANALYTICS

---

## 1. Data Cleaning Strategy (Base Layer)

The base layer focuses on cleaning and standardizing raw data while keeping it as close to the source as possible.  
We process a total of **8 source tables**, applying a consistent workflow to each.

### Key Steps

- **Source Referencing**
  - Use dbt `source()` to reference raw tables
  - Ensures clear data lineage and maintainability

- **Column Selection & Renaming**
  - Convert column names to `snake_case`
  - Rename unclear or inconsistent fields

- **Basic Data Cleaning**
  - Handle null values in key columns
  - Standardize data types (e.g., timestamps)

- **Metadata Handling**
  - Retain fields like `_loaded_at` for tracking data freshness

- **Minimal Transformation Principle**
  - Avoid heavy transformations
  - Keep data close to the raw source

Each cleaned table is stored as an individual dbt model in the base layer and serves as the foundation for further transformations.

---

## 2. dbt Modeling Structure

The project is organized into three layers:

### Base Layer
- Contains 8 cleaned tables (one per source table)
- Naming convention: `base_<source>_<table>`
- Purpose: standardized raw data

### Intermediate Layer
- Combines and transforms base tables
- Includes:
  - Table joins
  - Deduplication
  - Business logic transformations

### Analytics Layer
- Final business-ready datasets
- Includes:
  - Dimension tables (e.g., customers, employees, products)
  - Fact tables (e.g., orders, sessions)
  - Aggregated metrics (e.g., daily reports)

---

## 3. Development Workflow

- Version control managed with GitHub (feature branches + pull requests)
- Development performed in dbt Cloud (Development environment)
- Each developer uses an isolated schema (`dbt_<name>`)
- Models are validated using:
  - `dbt run`
  - `dbt test`

---

## 4. Production Deployment

- Separate Production environment configured in dbt Cloud
- Uses dedicated roles and database (`PROD`)
- Daily scheduled job runs:
  - `dbt run`
  - `dbt test`

This ensures consistent updates and data quality.

---

## 5. Data Reliability & Error Handling

To ensure pipeline stability:

- Monitor dbt job runs and failures
- Handle potential issues such as:
  - Data ingestion failures
  - Snowflake warehouse suspension
  - Model execution errors

### Debugging Approach
- Check dbt logs
- Validate upstream dependencies
- Re-run failed models

---

## Summary

This project implements a scalable and maintainable data pipeline by:

- Cleaning and standardizing raw data in the base layer
- Structuring transformations across intermediate and analytics layers
- Applying dbt best practices for modularity, testing, and deployment

The result is a reliable foundation for downstream analytics and business insights.
