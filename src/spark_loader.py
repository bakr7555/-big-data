"""
spark_loader.py - Apache PySpark DataFrame Loader for Large/Massive Datasets

Reads large CSV files using PySpark DataFrame API with strict explicit Schema (no inferSchema).
Guarantees robust memory management, graceful SparkSession cleanup with try/finally,
and distributed partition processing for high-volume ELT pipelines.
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from pymongo.database import Database

# Add project root to sys.path
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    COLLECTION_RAW,
    MONGO_DB_NAME,
    MONGO_URI,
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_MASTER,
    SPARK_MONGO_PACKAGE,
)
from src.schema import get_spark_explicit_schema, PYSPARK_AVAILABLE

if PYSPARK_AVAILABLE:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        current_timestamp,
        lit,
        monotonically_increasing_id,
        struct,
    )


def get_explicit_orders_schema() -> Any:
    """
    Returns explicit fixed Schema for the orders CSV.
    Avoids `inferSchema` overhead and preserves raw text integrity for dirty data cleaning.
    """
    return get_spark_explicit_schema()


def create_spark_session(app_name: str = SPARK_APP_NAME) -> Any:
    """
    Builds and configures a SparkSession optimized for MongoDB ELT streaming/batching.
    """
    if not PYSPARK_AVAILABLE:
        raise ImportError(
            "PySpark is not installed or available in this environment. "
            "Please install pyspark or use 'python_batch' engine."
        )

    print(f"\n[Spark Loader] Initializing SparkSession '{app_name}'...")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.mongodb.read.connection.uri", f"{MONGO_URI}{MONGO_DB_NAME}.{COLLECTION_RAW}")
        .config("spark.mongodb.write.connection.uri", f"{MONGO_URI}{MONGO_DB_NAME}.{COLLECTION_RAW}")
        .config("spark.jars.packages", SPARK_MONGO_PACKAGE)
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f" - Spark Version: {spark.version}")
    print(f" - Spark Master:  {SPARK_MASTER}")
    return spark


def load_raw_pyspark(
    file_path: str,
    run_id: str,
    mongo_uri: str = MONGO_URI,
    db_name: str = MONGO_DB_NAME,
    collection_name: str = COLLECTION_RAW,
) -> Dict[str, Any]:
    """
    Loads large CSV via PySpark DataFrame API, transforms into Raw Ingestion schema
    with metadata, and writes to MongoDB with try/finally session management.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for PySpark loading engine.")

    input_path = Path(file_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    start_time = time.time()
    spark = None

    try:
        spark = create_spark_session()
        schema = get_explicit_orders_schema()

        print(f"\n[PySpark Ingestion] Reading CSV with explicit schema: {input_path.name}")
        df = (
            spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv(str(input_path.resolve()))
        )

        num_partitions = df.rdd.getNumPartitions()
        print(f" - DataFrame Partitions: {num_partitions}")

        # Add ELT metadata columns
        df_raw = (
            df
            .withColumn("run_id", lit(run_id))
            .withColumn("source_file", lit(input_path.name))
            .withColumn("source_row_number", monotonically_increasing_id() + 1)
            .withColumn("ingested_at", current_timestamp().cast("string"))
            .withColumn("engine_used", lit("pyspark"))
            .withColumn("raw_record", struct([col(c) for c in df.columns]))
        )

        # Ingest to MongoDB by partition using PyMongo inside worker nodes
        def write_partition_to_mongo(partition_iter):
            import pymongo
            client = pymongo.MongoClient(mongo_uri)
            db_conn = client[db_name]
            raw_coll = db_conn[collection_name]
            
            records = []
            for row in partition_iter:
                row_dict = row.asDict(recursive=True)
                records.append(row_dict)
                if len(records) >= 5000:
                    raw_coll.insert_many(records, ordered=False)
                    records = []
            
            if records:
                raw_coll.insert_many(records, ordered=False)
            
            client.close()

        print(f" - Ingesting partitions into MongoDB '{collection_name}'...")
        df_raw.foreachPartition(write_partition_to_mongo)

        total_rows = df.count()
        duration = time.time() - start_time
        throughput = total_rows / duration if duration > 0 else 0

        print(f"\n[PySpark Load Completed]")
        print(f" - Ingested Rows: {total_rows:,}")
        print(f" - Elapsed Time:  {duration:.2f}s ({throughput:,.1f} rows/sec)\n")

        return {
            "engine": "pyspark",
            "total_raw_rows": total_rows,
            "partitions": num_partitions,
            "elapsed_seconds": round(duration, 2),
            "throughput_rows_per_sec": round(throughput, 1)
        }

    finally:
        if spark:
            print("[PySpark] Stopping SparkSession and releasing resources...")
            spark.stop()
            print("[PySpark] SparkSession stopped cleanly.")
