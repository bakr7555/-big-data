"""
metrics.py - Performance, Quality, and Consistency Metrics Tracker

Calculates throughput, validates the strict consistency equation:
  run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count
Tracks Upsert counters (inserted, updated, unchanged) and writes summary to reports/results.json.
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Add project root to sys.path
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import REPORTS_FILE_PATH


class PipelineMetricsTracker:
    """Tracks and validates all operational and quality metrics for an ELT execution run."""

    def __init__(self, run_id: str, source_file: str, engine_used: str, file_size_mb: float):
        self.run_id = run_id
        self.source_file = source_file
        self.engine_used = engine_used
        self.file_size_mb = file_size_mb
        self.start_time = time.time()
        self.start_iso = datetime.utcnow().isoformat()
        
        # Counters
        self.raw_count: int = 0
        self.valid_count: int = 0
        self.corrected_count: int = 0
        self.quarantine_count: int = 0

        # Upsert Counters
        self.upsert_inserted: int = 0
        self.upsert_updated: int = 0
        self.upsert_unchanged: int = 0

        # Diagnostics Breakdown
        self.error_counts: Dict[str, int] = {}
        self.rule_correction_counts: Dict[str, int] = {}
        self.config_details: Dict[str, Any] = {}

    def set_counts(self, raw: int, valid: int, corrected: int, quarantine: int) -> None:
        """Sets record classification counts."""
        self.raw_count = raw
        self.valid_count = valid
        self.corrected_count = corrected
        self.quarantine_count = quarantine

    def set_upsert_stats(self, inserted: int, updated: int, unchanged: int) -> None:
        """Sets idempotency upsert statistics."""
        self.upsert_inserted = inserted
        self.upsert_updated = updated
        self.upsert_unchanged = unchanged

    def increment_error(self, error_code: str) -> None:
        """Increments counter for a specific quarantine error code."""
        self.error_counts[error_code] = self.error_counts.get(error_code, 0) + 1

    def increment_rule_correction(self, rule_code: str) -> None:
        """Increments counter for an applied auto-cleaning rule."""
        self.rule_correction_counts[rule_code] = self.rule_correction_counts.get(rule_code, 0) + 1

    def verify_consistency_rule(self) -> Dict[str, Any]:
        """
        Validates the fundamental ELT consistency equation:
          run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count
        """
        calculated_sum = self.valid_count + self.corrected_count + self.quarantine_count
        is_consistent = (self.raw_count == calculated_sum)
        discrepancy = self.raw_count - calculated_sum

        return {
            "is_consistent": is_consistent,
            "raw_count": self.raw_count,
            "sum_processed": calculated_sum,
            "valid_count": self.valid_count,
            "corrected_count": self.corrected_count,
            "quarantine_count": self.quarantine_count,
            "discrepancy": discrepancy,
            "equation": (
                f"{self.raw_count} (raw) == {self.valid_count} (valid) + "
                f"{self.corrected_count} (corrected) + {self.quarantine_count} (quarantine)"
            )
        }

    def generate_report(self) -> Dict[str, Any]:
        """Generates comprehensive execution summary dictionary."""
        end_time = time.time()
        duration = end_time - self.start_time
        throughput = self.raw_count / duration if duration > 0 else 0
        consistency = self.verify_consistency_rule()

        report = {
            # Section 6.12 Official Required Metrics
            "run_id": self.run_id,
            "file_name": self.source_file,
            "file_size_mb": round(self.file_size_mb, 2),
            "engine_used": self.engine_used,
            "rows_read": self.raw_count,
            "raw_loaded": self.raw_count,
            "valid_count": self.valid_count,
            "corrected_count": self.corrected_count,
            "quarantine_count": self.quarantine_count,
            "elapsed_seconds": round(duration, 2),
            "throughput": round(throughput, 1),
            "batch_size": self.config_details.get("batch_size", 5000),
            "partitions": self.config_details.get("partitions", 1),
            "error_case_counts": self.error_counts,
            "inserted_count": self.upsert_inserted,
            "updated_count": self.upsert_updated,
            "unchanged_count": self.upsert_unchanged,

            # Additional Diagnostics & Consistency Verification
            "timestamps": {
                "started_at": self.start_iso,
                "completed_at": datetime.utcnow().isoformat(),
                "duration_seconds": round(duration, 2)
            },
            "performance": {
                "total_duration_seconds": round(duration, 2),
                "throughput_records_per_sec": round(throughput, 1),
            },
            "record_counts": {
                "run_raw_count": self.raw_count,
                "run_valid_count": self.valid_count,
                "run_corrected_count": self.corrected_count,
                "run_quarantine_count": self.quarantine_count,
            },
            "consistency_rule": consistency,
            "upsert_metrics": {
                "inserted_new": self.upsert_inserted,
                "updated_existing": self.upsert_updated,
                "unchanged": self.upsert_unchanged,
                "total_upsert_operations": self.upsert_inserted + self.upsert_updated + self.upsert_unchanged
            },
            "quarantine_breakdown": self.error_counts,
            "rule_corrections_breakdown": self.rule_correction_counts,
            "configuration": self.config_details
        }
        return report

    def save_to_file(self, target_path: Path = REPORTS_FILE_PATH) -> Path:
        """Saves metrics report into JSON file."""
        report = self.generate_report()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"[Metrics] Report saved to: {target_path}")
        return target_path

    def print_terminal_summary(self) -> None:
        """Prints a clean ASCII summary to stdout."""
        report = self.generate_report()
        c = report["consistency_rule"]
        status_sym = "[PASSED]" if c["is_consistent"] else "[FAILED]"

        print("\n" + "=" * 75)
        print(f"             ELT PIPELINE EXECUTION SUMMARY  ({self.run_id})")
        print("=" * 75)
        print(f" Source File:          {self.source_file} ({self.file_size_mb:.2f} MB)")
        print(f" Processing Engine:    {self.engine_used}")
        print(f" Execution Duration:   {report['performance']['total_duration_seconds']} seconds")
        print(f" Average Throughput:   {report['performance']['throughput_records_per_sec']:,.1f} records/sec")
        print("-" * 75)
        print(" RECORD COUNTS & CONSISTENCY RULE:")
        print(f"   * Raw Ingested (run_raw_count):        {self.raw_count:,}")
        print(f"   * Clean Valid (run_valid_count):        {self.valid_count:,}")
        print(f"   * Corrected (run_corrected_count):      {self.corrected_count:,}")
        print(f"   * Quarantined (run_quarantine_count):  {self.quarantine_count:,}")
        print(f" Consistency Rule Check: {status_sym}")
        print(f"   Equation: {c['equation']}")
        print("-" * 75)
        print(" IDEMPOTENCY & UPSERT METRICS (orders_validated):")
        print(f"   * Inserted (New):     {self.upsert_inserted:,}")
        print(f"   * Updated (Modified): {self.upsert_updated:,}")
        print(f"   * Unchanged:          {self.upsert_unchanged:,}")
        print("-" * 75)
        if self.rule_correction_counts:
            print(" TOP APPLIED AUTO-CLEANING RULES:")
            for rule, count in sorted(self.rule_correction_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"   - {rule:<30}: {count:,}")
        if self.error_counts:
            print("-" * 75)
            print(" QUARANTINE ERRORS BREAKDOWN:")
            for err, count in sorted(self.error_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"   - {err:<30}: {count:,}")
        print("=" * 75 + "\n")
