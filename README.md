# Hybrid Data Pipeline & Analytics Engine (Big Data Project)
## Midterm (Phase 1) + Final (Phase 2 - Complete Deliverable)

**Student Name / إعداد الطالب:** بكر مهيوب خالد سيف التبعي (Baker Mahyoub Khaled Saif Al-Tabei)  
**University / الجامعة:** جامعة الرازي - كلية الحاسوب وتكنولوجيا المعلومات  
**Course / المقرر:** البيانات الضخمة (Big Data - Practical)  

---


## 1. Key Features & Architecture

### Phase 1: Ingestion & ELT Cleaning Pipeline (Midterm - 18 Marks)
1. Intelligent Hybrid Router (file_router.py): Automatically routes workloads to python_batch or pyspark based on file size against a configurable threshold (SMALL_FILE_THRESHOLD_MB).
2. True ELT Paradigm: Raw ingestion into orders_raw without preliminary data loss or modification, followed by in-database transformations.
3. 10 Mandatory Quality Rules (quality_rules.py):
   - Arabic-to-ASCII digit and decimal conversion (R1_ARABIC_NUMERALS)
   - Currency symbol standardization to YER (R2_CURRENCY_NORMALIZATION)
   - Thousands separator removal (R3_THOUSANDS_SEPARATOR)
   - Arabic price words translation (R4_PRICE_WORDS_TRANSLATION)
   - Phone number normalization to Yemeni standard 9-digit format (R5_PHONE_STANDARDIZATION)
   - Email syntax repair for doubled symbols (R6_EMAIL_CLEANING)
   - Date format unification to ISO-8601 (R7_DATE_STANDARDIZATION)
   - Whitespace trimming and status dictionary normalization (R8_WHITESPACE_AND_SYNONYMS)
   - Total amount recalculation based on items sum and delivery cost (R9_TOTAL_AMOUNT_RECALCULATION)
   - Data type casting for item quantities inside JSON structures (R10_ITEMS_DATA_TYPES)
4. Audit Trail (corrections): Preserves complete field-level audit trail documenting field, original_value, corrected_value, and rule_code.
5. Dead-Letter Quarantine (orders_quarantine): Uncorrectable records (missing keys, corrupted JSON, impossible dates, negative values) are isolated with diagnostic error codes.
6. 100% Idempotency Guarantee: Stable business key order_id with Unique Index and atomic Upsert operations prevents duplicate records on repeated runs.
7. Strict Consistency Verification: run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count

### Phase 2: Analytics, Indexes, Materialized Views, Scheduler & API (Final - 7 Marks)
1. Production Database Indexes & Performance Benchmarking (src/indexes.py):
   - Single-field index on customer_id
   - Compound index on (order_date, status)
   - Compound index on (city, total_amount)
   - Multikey index on items.sku
   - Built-in executionStats before/after comparison measuring execution time and examined document reduction.
2. 5 Business Queries (src/queries.py):
   - Q1_TOP_CUSTOMERS: VIP customers ranking by gross spending.
   - Q2_PENDING_ORDERS: Chronological tracking of unfulfilled and pending orders.
   - Q3_CITY_ORDERS_DISTRIBUTION: Regional volume and high-value orders filtering.
   - Q4_PRODUCT_SKU_SEARCH: Product lookup within nested items arrays.
   - Q5_PAYMENT_METHODS_AUDIT: Audit of payment channels, volume, and verification status.
3. 5 Aggregation Analytics Reports (src/aggregations.py):
   - sales_by_city: Sales volume, revenue, and average order value per city.
   - top_products: Units sold and gross revenue per SKU using $unwind.
   - top_customers: High-value customer ranking and lifetime volume.
   - monthly_sales_trend: Monthly financial growth velocity and shipping costs.
   - order_status_distribution: Fulfillment stage breakdown.
4. 2 Incremental Materialized Views (src/materialized_views.py):
   - daily_sales_summary: Daily aggregated revenue, order counts, and averages.
   - top_products_summary: SKU-level volume and revenue aggregates.
   - Incremental Refresh Strategy: High-watermark timestamp tracking in mv_refresh_metadata and atomic $inc updates without full rebuilding.
5. 2 Background Scheduled Jobs (src/scheduler.py):
   - refresh_materialized_views_job: Periodic incremental sync of summary views.
   - daily_performance_report_job: System metrics and throughput aggregation.
   - Full execution audit logging in job_execution_logs.
6. Unified FastAPI REST Interface (src/api.py):
   - Interactive Swagger documentation accessible at /docs.
   - 10 standardized routes for pipeline control, indexing, queries, aggregations, views, and jobs.

---

## 2. Project Directory Structure

```plaintext
midterm-data-pipeline/
|-- .env.example
|-- requirements.txt
|-- README.md
|-- midterm_pipeline.ipynb
|-- config/
|   |-- __init__.py
|   `-- settings.py
|-- data/
|   |-- .gitkeep
|   `-- orders_sample.csv
|-- src/
|   |-- __init__.py
|   |-- main.py
|   |-- file_router.py
|   |-- create_small_sample.py
|   |-- batch_loader.py
|   |-- spark_loader.py
|   |-- quality_rules.py
|   |-- elt_pipeline.py
|   |-- mongo_setup.py
|   |-- reprocess_quarantine.py
|   |-- show_duplicates_demo.py
|   |-- metrics.py
|   |-- indexes.py
|   |-- queries.py
|   |-- aggregations.py
|   |-- materialized_views.py
|   |-- scheduler.py
|   `-- api.py
|-- tests/
|   |-- __init__.py
|   |-- test_cleaning_rules.py
|   |-- test_classification.py
|   |-- test_idempotency.py
|   `-- test_final_features.py
|-- reports/
|   |-- results.json
|   `-- screenshots/
`-- docs/
    |-- architecture.md
    `-- final_project_documentation.md
```

---

## 3. Installation & Setup

### 1. Install Required Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables
```bash
cp .env.example .env
```
Ensure MongoDB Server is active locally on default port 27017 (mongodb://localhost:27017/).

---

## 4. Execution Commands

### 1. Run the ELT Data Pipeline
```bash
# Ingest default sample dataset with clean database reset:
python src/main.py --reset-db

# Ingest specific dataset file:
python src/main.py -f "data/orders_sample.csv" --reset-db
```

### 2. Run Database Indexing & Performance Benchmark
```bash
python src/indexes.py
```

### 3. Execute Business Queries
```bash
python src/queries.py
```

### 4. Execute Aggregation Reports
```bash
python src/aggregations.py
```

### 5. Refresh Materialized Views Incrementally
```bash
python src/materialized_views.py
```

### 6. Run Background Job Scheduler
```bash
python src/scheduler.py
```

### 7. Launch Unified FastAPI Server
```bash
python -m uvicorn src.api:app --reload --port 8000
```
Access the interactive Swagger UI documentation at: http://localhost:8000/docs

### 8. Run Full Automated Test Suite (21 Unit & Integration Tests)
```bash
python -m unittest discover tests
```

---

## 5. API Endpoints Reference

| HTTP Method | Endpoint Path | Description |
| :--- | :--- | :--- |
| GET | /health | System status, MongoDB connection, and collection counts. |
| POST | /ingest | Triggers the ELT data pipeline for a specified input file. |
| POST | /indexes | Builds all 4 indexes and executes the explain benchmark. |
| GET | /queries | Lists all 5 available business queries. |
| GET | /queries/{name} | Executes a specific query with optional filter parameters. |
| GET | /aggregations | Lists all 5 aggregation reports. |
| GET | /aggregations/{name} | Executes an analytical aggregation pipeline. |
| POST | /refresh-mv | Triggers incremental refresh for Materialized Views. |
| GET | /jobs | Lists registered jobs and recent execution history logs. |
| POST | /jobs/{name}/run | Manually triggers immediate execution of a background job. |
