"""
batch_loader.py - Python Streaming Batch Loader for Small-to-Medium CSVs

Streams CSV line-by-line using Python standard `csv.DictReader` and ingests in batches
via PyMongo `insert_many`. Guarantees strict O(1) memory complexity (no Pandas, no full file buffering).
"""

import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Generator, List, Optional
from pymongo.database import Database

# Add project root to sys.path
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import BATCH_SIZE, COLLECTION_RAW


def stream_csv_batches(
    file_path: Path,
    batch_size: int = BATCH_SIZE,
    encoding: str = "utf-8-sig"
) -> Generator[List[Dict[str, str]], None, None]:
    """
    Generator that streams a CSV file row-by-row and yields chunks of `batch_size`.
    Preserves strict O(batch_size) memory, never loads entire dataset into RAM.
    """
    batch: List[Dict[str, str]] = []
    
    with open(file_path, mode="r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            batch.append(dict(row))
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch



def load_raw_streaming_batch(
    db: Database,
    file_path: str,
    run_id: str,
    batch_size: int = BATCH_SIZE,
    encoding: str = "utf-8-sig"
) -> Dict[str, any]:
    """
    Performs the Raw Load step of ELT using Python Streaming Batching.
    Inserts raw documents into `orders_raw` collection with audit metadata.

    Metadata attached to each raw document:
      - run_id: Unique UUID/timestamp for the execution
      - source_file: Basename of the incoming CSV
      - source_row_number: Exact 1-indexed row number from CSV
      - ingested_at: ISO 8601 ingestion timestamp
      - engine_used: 'python_batch'
      - raw_record: The unmodified raw dictionary from CSV
    """
    input_path = Path(file_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    raw_collection = db[COLLECTION_RAW]
    file_size_mb = input_path.stat().st_size / (1024 * 1024)
    start_time = time.time()
    
    print(f"\n[Raw Load - Python Batch] Starting ingestion...")
    print(f" - Target Collection: {COLLECTION_RAW}")
    print(f" - Source File:       {input_path.name} ({file_size_mb:.2f} MB)")
    print(f" - Batch Size:        {batch_size:,} records")
    print(f" - Run ID:            {run_id}")

    total_rows = 0
    batch_idx = 0
    try:
        for chunk in stream_csv_batches(input_path, batch_size=batch_size, encoding=encoding):
            batch_idx += 1
            batch_docs = []
            
            for row in chunk:
                total_rows += 1
                doc = {
                    "order_id": row.get("order_id"),
                    "run_id": run_id,
                    "source_file": input_path.name,
                    "source_row_number": total_rows,
                    "ingested_at": datetime.utcnow().isoformat(),
                    "engine_used": "python_batch",
                    "raw_record": row,
                }
                batch_docs.append(doc)

            # Insert raw batch into MongoDB
            if batch_docs:
                raw_collection.insert_many(batch_docs, ordered=False)

            elapsed = time.time() - start_time
            current_throughput = total_rows / elapsed if elapsed > 0 else 0
            print(
                f"   [Batch #{batch_idx:03d}] Ingested {len(batch_docs):,} raw rows "
                f"(Total: {total_rows:,} | {current_throughput:,.1f} rows/sec)"
            )

    except Exception as err:
        print(f"[ERROR] Raw load interrupted at row {total_rows}: {err}")
        raise
    total_duration = time.time() - start_time
    final_throughput = total_rows / total_duration if total_duration > 0 else 0
    print(f"\n[Raw Load Completed]")
    print(f" - Total Raw Records Ingested: {total_rows:,}")
    print(f" - Total Batches:              {batch_idx}")
    print(f" - Elapsed Time:               {total_duration:.2f}s")
    print(f" - Average Ingestion Speed:    {final_throughput:,.1f} rows/sec\n")
    return {
        "engine": "python_batch",
        "total_raw_rows": total_rows,
        "total_batches": batch_idx,
        "batch_size": batch_size,
        "elapsed_seconds": round(total_duration, 2),
        "throughput_rows_per_sec": round(final_throughput, 1)
    }
