"""
schema.py - Explicit Schema Definitions for PySpark and MongoDB

Defines:
  1. Strict Explicit PySpark DataFrame Schema (zero inferSchema, sensitive fields as StringType).
  2. MongoDB Document Schemas for Raw, Validated, and Quarantine collections.
"""

from typing import Any

# PySpark Schema Types
try:
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


def get_spark_explicit_schema() -> Any:
    """
    Returns explicit fixed StructType Schema for the orders dataset in PySpark.
    All 17 fields are explicitly defined as StringType to prevent automatic casting
    and preserve raw dirty data (Arabic numerals, symbols, corrupted text) for ELT transformation.
    """
    if not PYSPARK_AVAILABLE:
        return None

    return StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("district", StringType(), True),
        StructField("delivery_type", StringType(), True),
        StructField("delivery_cost", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("items_json", StringType(), True),
    ])


# Standard MongoDB Expected Fields
RAW_COLLECTION_FIELDS = [
    "run_id",
    "source_file",
    "source_row_number",
    "ingested_at",
    "engine_used",
    "raw_record"
]

VALIDATED_COLLECTION_FIELDS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items",
    "quality_status",
    "corrections",
    "processed_at"
]

QUARANTINE_COLLECTION_FIELDS = [
    "order_id",
    "error_code",
    "error_reason",
    "raw_record",
    "quarantined_at",
    "run_id",
    "source_file"
]
