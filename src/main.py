"""
main.py - Main Command-Line Entrypoint for Hybrid Orders Data Pipeline (ELT)

Usage Examples:
  # 1. Automatic Hybrid Routing (Default):
  python src/main.py -f data/orders_sample.csv

  # 2. Process Large CSV file:
  python src/main.py -f "H:/midterm-data-pipeline/data/orders_huge_mixed_quality.csv"

  # 3. Explicit Engine Selection:
  python src/main.py -f data/orders_sample.csv --engine python_batch

  # 4. Fresh Run (Clearing MongoDB collections):
  python src/main.py -f data/orders_sample.csv --reset-db
"""
import argparse
import sys
from pathlib import Path
# Add project root to sys.path
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from config.settings import BATCH_SIZE, DATA_DIR, SMALL_FILE_THRESHOLD_MB
from src.elt_pipeline import run_pipeline


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hybrid Data Pipeline (ELT Architecture) for Orders Ingestion & Quality Processing"
    )

    default_file = DATA_DIR / "orders_sample.csv"
    huge_file = Path("H:/midterm-data-pipeline/data/orders_huge_mixed_quality.csv")
    if not default_file.exists() and huge_file.exists():
        default_file = huge_file
    parser.add_argument(
        "-f", "--file",
        type=str,
        default=str(default_file),
        help="Path to the input orders CSV data file."
    )
    parser.add_argument(
        "-e", "--engine",
        type=str,
        choices=["auto", "python_batch", "pyspark"],
        default="auto",
        help="Engine selection: 'auto' for intelligent router, or explicit override ('python_batch', 'pyspark')."
    )
    parser.add_argument(
        "-b", "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Batch size for streaming insert_many and bulk operations (default: {BATCH_SIZE})."
    )
    parser.add_argument(
        "-t", "--threshold",
        type=float,
        default=SMALL_FILE_THRESHOLD_MB,
        help=f"File size threshold in MB to switch engines (default: {SMALL_FILE_THRESHOLD_MB} MB)."
    )
    parser.add_argument(
        "--reset-db",
        action="store_true",
        help="Drop and re-initialize pipeline collections before ingestion."
    )
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Inspect and display live validated, quarantined, and duplicated records from MongoDB."
    )

    return parser.parse_args()

def main():
    args = parse_arguments()

    if args.inspect:
        from src.inspect_live_data import inspect_all_data_layers
        inspect_all_data_layers()
        sys.exit(0)

    print("=" * 80)
    print("        HYBRID DATA PIPELINE (ELT ARCHITECTURE) - BIG DATA PROJECT")
    print("=" * 80)
    print(f" Input File:       {args.file}")
    print(f" Routing Mode:     {args.engine}")
    print(f" Batch Size:       {args.batch_size:,}")
    print(f" Size Threshold:   {args.threshold} MB")
    print(f" Reset Collections:{args.reset_db}")
    print("=" * 80)
    engine_override = None if args.engine == "auto" else args.engine
    try:
        report = run_pipeline(
            file_path=args.file,
            engine_override=engine_override,
            batch_size=args.batch_size,
            threshold_mb=args.threshold,
            reset_db=args.reset_db
        )
        print("[SUCCESS] ELT Pipeline execution finished successfully!")
        sys.exit(0)
    except Exception as exc:
        print(f"\n[FATAL ERROR] Pipeline execution failed: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
