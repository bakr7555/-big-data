# Hybrid Data Pipeline (ELT Architecture) - Big Data Project

A production-grade **Hybrid Data Pipeline** following the **ELT (Extract, Load, Transform)** architecture. Built using Python Streaming Batch Processing, Apache PySpark DataFrame API, and MongoDB (PyMongo).

---

## 🌟 Key Features

1. **Intelligent Hybrid Router (`file_router.py`):** Automatically routes workloads to `python_batch` or `pyspark` based on file size against a configurable threshold (`SMALL_FILE_THRESHOLD_MB`).
2. **True ELT Paradigm:** Raw ingestion into `orders_raw` without preliminary data loss or modification, followed by in-database transformations.
3. **8 Mandatory Auto-Cleaning Rules (`quality_rules.py`):**
   - Arabic-to-ASCII digit and decimal conversion (`R1_ARABIC_NUMERALS`).
   - Currency symbol stripping and standardization to `YER` (`R2_CURRENCY_NORMALIZATION`).
   - Thousands separator removal (`R3_THOUSANDS_SEPARATOR`).
   - Arabic price words translation (`R4_PRICE_WORDS_TRANSLATION`).
   - Phone number normalization to Yemeni standard 9-digit format (`R5_PHONE_STANDARDIZATION`).
   - Email syntax repair for doubled symbols (`R6_EMAIL_SYNTAX_REPAIR`).
   - Date format unification to ISO-8601 (`R7_DATE_STANDARDIZATION`).
   - Whitespace trimming and status dictionary normalization (`R8_WHITESPACE_AND_SYNONYMS`).
4. **Audit Trail (`corrections`):** Preserves complete field-level audit trail documenting `field`, `original_value`, `corrected_value`, and `rule_code`.
5. **Dead-Letter Quarantine (`orders_quarantine`):** Uncorrectable records (missing keys, corrupted JSON, impossible dates) are safely isolated with diagnostic error codes.
6. **100% Idempotency Guarantee:** Stable business key `order_id` with Unique Index and Upsert operations prevents duplicate insertion on repeated runs.
7. **Strict Consistency Verification:**
   $$\text{run\_raw\_count} = \text{run\_valid\_count} + \text{run\_corrected\_count} + \text{run\_quarantine\_count}$$
8. **Performance Reporting:** Generates full execution statistics and writes summary to `reports/results.json`.

---

## 📁 Project Directory Structure

```plaintext
midterm-data-pipeline/
|-- README.md
|-- requirements.txt
|-- config/
|   |-- __init__.py
|   `-- settings.py
|-- data/
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
|   `-- metrics.py
|-- tests/
|   |-- test_cleaning_rules.py
|   `-- test_classification.py
|-- reports/
|   `-- results.json
`-- docs/
    `-- architecture.md
```

---

## 🚀 Quickstart Guide

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Extract a Sample from Huge CSV (No Pandas, O(1) / O(k) Memory)
```bash
# Fast extract first 5,000 rows
python src/create_small_sample.py -i "H:/midterm-data-pipeline/data/orders_huge_mixed_quality.csv" -o "data/orders_sample.csv" -n 5000 --head

# Or Uniform Reservoir Sampling:
python src/create_small_sample.py -i "H:/midterm-data-pipeline/data/orders_huge_mixed_quality.csv" -o "data/orders_sample.csv" -n 2000
```

### 3. Run the Complete ELT Pipeline
```bash
# Automatic Hybrid Routing
python src/main.py -f data/orders_sample.csv

# Process Huge Dataset with PySpark
python src/main.py -f "H:/midterm-data-pipeline/data/orders_huge_mixed_quality.csv"

# Reset database collections before ingestion
python src/main.py -f data/orders_sample.csv --reset-db
```

### 4. Run Unit Tests
```bash
python -m unittest discover tests
```

---

## 📊 Sample Execution Output & Results

```plaintext
===========================================================================
             ELT PIPELINE EXECUTION SUMMARY  (run_20260828_204752_7e3e18)
===========================================================================
 Source File:          orders_sample.csv (0.83 MB)
 Processing Engine:    python_batch
 Execution Duration:   1.67 seconds
 Average Throughput:   1,198.3 records/sec
---------------------------------------------------------------------------
 RECORD COUNTS & CONSISTENCY RULE:
   * Raw Ingested (run_raw_count):        2,000
   * Clean Valid (run_valid_count):        541
   * Corrected (run_corrected_count):      1,392
   * Quarantined (run_quarantine_count):  67
 Consistency Rule Check: [PASSED]
   Equation: 2000 (raw) == 541 (valid) + 1392 (corrected) + 67 (quarantine)
---------------------------------------------------------------------------
 IDEMPOTENCY & UPSERT METRICS (orders_validated):
   * Inserted (New):     1,918
   * Updated (Modified): 15
   * Unchanged:          0
---------------------------------------------------------------------------
 TOP APPLIED AUTO-CLEANING RULES:
   - R8_WHITESPACE_AND_SYNONYMS    : 1,287
   - R7_DATE_STANDARDIZATION       : 66
   - R1_ARABIC_NUMERALS            : 64
   - R6_EMAIL_SYNTAX_REPAIR        : 46
   - R5_PHONE_STANDARDIZATION      : 44
   - R2_CURRENCY_NORMALIZATION     : 22
   - R4_PRICE_WORDS_TRANSLATION    : 22
   - R3_THOUSANDS_SEPARATOR        : 20
===========================================================================
```
