"""
elt_pipeline.py - Core Orchestrator for the Hybrid ELT Pipeline

Executes the end-to-end ELT lifecycle:
  1. Raw Ingestion (orders_raw) with full tracking metadata.
  2. In-Database Transformation & Quality Processing (8 Auto-Cleaning Rules).
  3. Quarantine Isolation (orders_quarantine) for uncorrectable errors.
  4. Idempotent Upsert (orders_validated) on Unique Key `order_id`.
  5. Strict Consistency Rule Validation and Metrics Export to reports/results.json.
"""

import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from pymongo import UpdateOne
from pymongo.database import Database

# Add project root to sys.path
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    BATCH_SIZE,
    COLLECTION_QUARANTINE,
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
    REPORTS_FILE_PATH,
    SMALL_FILE_THRESHOLD_MB,
)
from src.batch_loader import load_raw_streaming_batch
from src.file_router import ENGINE_PYTHON_BATCH, ENGINE_PYSPARK, route_file
from src.metrics import PipelineMetricsTracker
from src.mongo_setup import get_database, get_mongo_client, setup_mongo_collections
from src.quality_rules import process_and_classify_record


def generate_run_id() -> str:
    """Generates unique run ID containing timestamp and short UUID."""
    now_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    short_uuid = uuid.uuid4().hex[:6]
    return f"run_{now_str}_{short_uuid}"


def run_pipeline(
    file_path: str,
    engine_override: Optional[str] = None,
    batch_size: int = BATCH_SIZE,
    threshold_mb: float = SMALL_FILE_THRESHOLD_MB,
    reset_db: bool = False,
) -> Dict[str, Any]:
    """
    Executes the Complete ELT Pipeline.

    Args:
        file_path: Path to the input orders CSV file.
        engine_override: Optional manual engine selection ('python_batch' or 'pyspark').
        batch_size: Batch size for streaming operations.
        threshold_mb: File size threshold in MB for router.
        reset_db: If True, clears existing pipeline collections before running.

    Returns:
        Summary report dictionary.
    """
    input_path = Path(file_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Cannot find input file: {file_path}")

    run_id = generate_run_id()
    file_size_mb = input_path.stat().st_size / (1024 * 1024)

    # 1. File Router Decision
    if engine_override:
        engine_used = engine_override.lower()
        print(f"\n[File Router] Manual override used: '{engine_used}'")
    else:
        engine_used, _ = route_file(str(input_path), threshold_mb)

    metrics = PipelineMetricsTracker(
        run_id=run_id,
        source_file=input_path.name,
        engine_used=engine_used,
        file_size_mb=file_size_mb
    )
    metrics.config_details = {
        "batch_size": batch_size,
        "threshold_mb": threshold_mb,
        "engine_override": engine_override,
    }

    client = None
    try:
        # Connect to MongoDB and ensure indexes
        client = get_mongo_client()
        db = get_database(client)

        if reset_db:
            from src.mongo_setup import reset_collections
            reset_collections(db)
        else:
            setup_mongo_collections(db)

        # -----------------------------------------------------------------
        # STEP 1: RAW INGESTION (E & L)
        # -----------------------------------------------------------------
        print(f"\n>>> STEP 1: Ingesting Raw Data into '{COLLECTION_RAW}'...")
        if engine_used == ENGINE_PYSPARK:
            try:
                from src.spark_loader import load_raw_pyspark
                spark_res = load_raw_pyspark(str(input_path), run_id=run_id)
                raw_ingested_count = spark_res["total_raw_rows"]
            except Exception as spark_err:
                print(f"[WARN] PySpark execution failed ({spark_err}). Falling back to Python Streaming Batch...")
                engine_used = ENGINE_PYTHON_BATCH
                metrics.engine_used = "python_batch (fallback)"
                batch_res = load_raw_streaming_batch(db, str(input_path), run_id=run_id, batch_size=batch_size)
                raw_ingested_count = batch_res["total_raw_rows"]
        else:
            batch_res = load_raw_streaming_batch(db, str(input_path), run_id=run_id, batch_size=batch_size)
            raw_ingested_count = batch_res["total_raw_rows"]

        # -----------------------------------------------------------------
        # STEP 2 & 3: TRANSFORM & QUALITY AUDITING (T) & QUARANTINE / UPSERT
        # -----------------------------------------------------------------
        print(f"\n>>> STEP 2: Processing Quality Rules, Auditing & Idempotent Upsert...")
        raw_col = db[COLLECTION_RAW]
        val_col = db[COLLECTION_VALIDATED]
        quar_col = db[COLLECTION_QUARANTINE]

        # Query raw documents for this specific run
        raw_cursor = raw_col.find({"run_id": run_id}, no_cursor_timeout=True)

        valid_count = 0
        corrected_count = 0
        quarantine_count = 0

        upsert_inserted = 0
        upsert_updated = 0
        upsert_unchanged = 0

        quarantine_buffer: List[Dict[str, Any]] = []
        upsert_operations: List[UpdateOne] = []

        processed_so_far = 0
        t_start_transform = time.time()

        for doc in raw_cursor:
            processed_so_far += 1
            raw_record = doc.get("raw_record", {})
            source_row_num = doc.get("source_row_number", processed_so_far)

            # Classify record and apply 8 cleaning rules
            classification_result = process_and_classify_record(raw_record)
            classification = classification_result["classification"]
            rec = classification_result["record"]

            # Attach run audit metadata
            rec["run_id"] = run_id
            rec["source_file"] = input_path.name
            rec["source_row_number"] = source_row_num

            if classification == "QUARANTINE":
                quarantine_count += 1
                err_code = classification_result.get("error_code") or "CORRUPTED_RECORD"
                metrics.increment_error(err_code)
                quarantine_buffer.append(rec)

            elif classification == "CORRECTED":
                corrected_count += 1
                # Track each applied rule in metrics
                for corr in rec.get("corrections", []):
                    metrics.increment_rule_correction(corr["rule_code"])

                # Prepare idempotent upsert
                upsert_operations.append(
                    UpdateOne({"order_id": rec["order_id"]}, {"$set": rec}, upsert=True)
                )

            else:  # VALID
                valid_count += 1
                upsert_operations.append(
                    UpdateOne({"order_id": rec["order_id"]}, {"$set": rec}, upsert=True)
                )

            # Flush quarantine batch immediately (e.g. at 500 records for real-time visibility in Compass)
            if len(quarantine_buffer) >= 500:
                quar_col.insert_many(quarantine_buffer, ordered=False)
                quarantine_buffer = []

            # Flush upsert operations batch
            if len(upsert_operations) >= batch_size:
                bulk_result = val_col.bulk_write(upsert_operations, ordered=False)
                upsert_inserted += bulk_result.upserted_count
                upsert_updated += bulk_result.modified_count
                upsert_unchanged += (bulk_result.matched_count - bulk_result.modified_count)
                upsert_operations = []

            if processed_so_far % 25_000 == 0:
                elapsed_t = time.time() - t_start_transform
                spd = processed_so_far / elapsed_t if elapsed_t > 0 else 0
                print(
                    f"   ... Evaluated {processed_so_far:,} records "
                    f"(Valid: {valid_count:,} | Corrected: {corrected_count:,} | "
                    f"Quarantine: {quarantine_count:,} | Speed: {spd:,.1f} rec/s)"
                )

        # Flush remaining buffers
        if quarantine_buffer:
            quar_col.insert_many(quarantine_buffer, ordered=False)
            quarantine_buffer = []

        if upsert_operations:
            bulk_result = val_col.bulk_write(upsert_operations, ordered=False)
            upsert_inserted += bulk_result.upserted_count
            upsert_updated += bulk_result.modified_count
            upsert_unchanged += (bulk_result.matched_count - bulk_result.modified_count)
            upsert_operations = []

        raw_cursor.close()

        # -----------------------------------------------------------------
        # STEP 4: RECORD COUNTS & CONSISTENCY CHECK
        # -----------------------------------------------------------------
        metrics.set_counts(
            raw=raw_ingested_count,
            valid=valid_count,
            corrected=corrected_count,
            quarantine=quarantine_count,
        )
        metrics.set_upsert_stats(
            inserted=upsert_inserted,
            updated=upsert_updated,
            unchanged=upsert_unchanged,
        )

        # Verify the consistency equation
        consistency = metrics.verify_consistency_rule()
        if not consistency["is_consistent"]:
            print(f"[FATAL CONSISTENCY ERROR] {consistency['equation']} (Discrepancy: {consistency['discrepancy']})")
        else:
            print(f"[CONSISTENCY VERIFIED] {consistency['equation']}")

        # -----------------------------------------------------------------
        # STEP 5: SAVE METRICS & PRINT SUMMARY
        # -----------------------------------------------------------------
        metrics.save_to_file(REPORTS_FILE_PATH)
        metrics.print_terminal_summary()

        return metrics.generate_report()

    finally:
        if client:
            client.close()
            print("[ELT Pipeline] MongoDB client connection closed.")
