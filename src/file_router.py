"""
file_router.py - Intelligent Engine Router for Hybrid ELT Pipeline

Inspects input file size and automatically routes the execution:
  - If File Size < SMALL_FILE_THRESHOLD_MB -> python_batch (Streamed batch loading, zero JVM overhead)
  - If File Size >= SMALL_FILE_THRESHOLD_MB -> pyspark (Distributed parallel processing)
Prints clear technical justification for the chosen engine.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Tuple

# Add project root to sys.path
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import SMALL_FILE_THRESHOLD_MB

ENGINE_PYTHON_BATCH = "python_batch"
ENGINE_PYSPARK = "pyspark"


def route_file(file_path: str, threshold_mb: float = SMALL_FILE_THRESHOLD_MB) -> Tuple[str, Dict[str, any]]:
    """
    Evaluates file size against the threshold and selects the optimal processing engine.

    Args:
        file_path: Path to CSV data file.
        threshold_mb: Size threshold in Megabytes.

    Returns:
        (selected_engine, routing_metadata)
    """
    path_obj = Path(file_path)
    if not path_obj.exists():
        raise FileNotFoundError(f"Source file not found at: {file_path}")

    size_bytes = path_obj.stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    if size_mb < threshold_mb:
        selected_engine = ENGINE_PYTHON_BATCH
        justification = (
            f"File size ({size_mb:.2f} MB) is strictly below threshold ({threshold_mb:.2f} MB). "
            f"Selected 'python_batch' engine for ultra-low latency, zero JVM startup overhead, "
            f"and strict O(1) streaming batch ingestion into MongoDB."
        )
    else:
        selected_engine = ENGINE_PYSPARK
        justification = (
            f"File size ({size_mb:.2f} MB) meets or exceeds threshold ({threshold_mb:.2f} MB). "
            f"Selected 'pyspark' engine for distributed DataFrame partitioning, multi-threaded "
            f"parallel execution, and high-throughput ELT scaling."
        )

    routing_metadata = {
        "file_path": str(path_obj.resolve()),
        "file_name": path_obj.name,
        "size_bytes": size_bytes,
        "size_mb": round(size_mb, 2),
        "threshold_mb": threshold_mb,
        "selected_engine": selected_engine,
        "justification": justification
    }

    # Print user-visible router summary
    print("\n" + "=" * 70)
    print(" [FILE ROUTER] Hybrid Data Pipeline Ingestion Decision")
    print("=" * 70)
    print(f" - Input File:       {path_obj.name} ({size_mb:.2f} MB / {size_bytes:,} bytes)")
    print(f" - Threshold:        {threshold_mb:.2f} MB")
    print(f" - Selected Engine:  [{selected_engine.upper()}]")
    print(f" - Justification:    {justification}")
    print("=" * 70 + "\n")

    return selected_engine, routing_metadata


def main():
    parser = argparse.ArgumentParser(description="Hybrid ELT Pipeline File Router")
    parser.add_argument("-f", "--file", type=str, required=True, help="Path to input CSV file")
    parser.add_argument("-t", "--threshold", type=float, default=SMALL_FILE_THRESHOLD_MB, help="Threshold in MB")
    args = parser.parse_args()

    engine, metadata = route_file(args.file, args.threshold)
    print(f"Result: {engine}")


if __name__ == "__main__":
    main()
