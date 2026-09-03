"""
create_small_sample.py - Memory-Efficient CSV Sampler (Streaming Reservoir Sampling)

This script extracts a representative random sample from massive CSV files without loading
the entire file into memory (strictly O(1) or O(k) memory complexity).
Pandas is intentionally NOT used to guarantee zero memory overflow on large datasets.
"""
import argparse
import csv
import os
import random
import sys
import time
from pathlib import Path

# Add project root to sys.path if executed directly
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DATA_DIR
def extract_sample_reservoir(
    input_file: Path,
    output_file: Path,
    sample_size: int =  10000,
    seed: int = None,
    encoding: str = "utf-8"
) -> dict:
    """
    Extracts a random sample of exact size `sample_size` using Reservoir Sampling (Algorithm R).
    Memory complexity: O(sample_size) records in memory, independent of input file size.

    Args:
        input_file: Path to source large CSV.
        output_file: Path to destination sample CSV.
        sample_size: Number of records to extract.
        seed: Optional random seed for reproducible sampling.
        encoding: File encoding (defaults to utf-8, handles Arabic text).
    Returns:
        Dictionary containing execution statistics and metrics.
    """
    if seed is not None:
        random.seed(seed)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    start_time = time.time()
    input_size_mb = input_file.stat().st_size / (1024 * 1024)
    print(f"\n[Sampler] Starting Reservoir Sampling...")
    print(f" - Input File:  {input_file} ({input_size_mb:.2f} MB)")
    print(f" - Target Sample Size: {sample_size:,} rows")
    print(f" - Random Seed: {seed}")

    reservoir = []
    header = None
    total_data_rows = 0

    # Stream the input file line-by-line using Python standard csv module
    with open(input_file, mode="r", encoding=encoding, errors="replace", newline="") as infile:
        reader = csv.reader(infile)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(f"Input file {input_file} is empty!")

        for row in reader:
            if not row:  # skip empty lines
                continue

            if total_data_rows < sample_size:
                reservoir.append(row)
            else:
                # Reservoir Sampling logic: pick random index between 0 and total_data_rows
                j = random.randint(0, total_data_rows)
                if j < sample_size:
                    reservoir[j] = row

            total_data_rows += 1
            if total_data_rows % 100_000 == 0:
                print(f"   ... Streamed {total_data_rows:,} rows so far (O(k) memory preserved)")

    actual_sample_count = len(reservoir)

    # Ensure parent directory for output exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Write sampled rows to target CSV
    with open(output_file, mode="w", encoding=encoding, errors="replace", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)
        writer.writerows(reservoir)

    elapsed_time = time.time() - start_time
    output_size_mb = output_file.stat().st_size / (1024 * 1024)
    throughput = total_data_rows / elapsed_time if elapsed_time > 0 else 0

    print(f"\n[Sampler Completed Successfully]")
    print(f" - Total Input Rows Processed: {total_data_rows:,}")
    print(f" - Sample Rows Written:        {actual_sample_count:,}")
    print(f" - Output File:                {output_file} ({output_size_mb:.3f} MB)")
    print(f" - Elapsed Time:               {elapsed_time:.2f}s ({throughput:,.1f} rows/sec)\n")

    return {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "input_size_mb": round(input_size_mb, 2),
        "output_size_mb": round(output_size_mb, 3),
        "total_input_rows": total_data_rows,
        "sample_rows": actual_sample_count,
        "elapsed_seconds": round(elapsed_time, 2),
        "throughput_rows_per_sec": round(throughput, 1)
    }


def extract_sample_ratio(
    input_file: Path,
    output_file: Path,
    ratio: float = 0.05,
    seed: int = None,
    encoding: str = "utf-8"
) -> dict:
    """
    Extracts a random fraction of records using Bernoulli streaming sampling.
    Memory complexity: Strict O(1) memory, writes records immediately as they match.

    Args:
        input_file: Path to source large CSV.
        output_file: Path to destination sample CSV.
        ratio: Sampling probability between 0.0 and 1.0 (e.g. 0.05 = 5%).
        seed: Optional random seed for reproducibility.
        encoding: File encoding.

    Returns:
        Dictionary containing execution statistics and metrics.
    """
    if seed is not None:
        random.seed(seed)

    if not (0.0 < ratio <= 1.0):
        raise ValueError(f"Sampling ratio must be between 0 and 1, got {ratio}")

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    start_time = time.time()
    input_size_mb = input_file.stat().st_size / (1024 * 1024)
    print(f"\n[Sampler] Starting Streaming Bernoulli Sampling...")
    print(f" - Input File:  {input_file} ({input_size_mb:.2f} MB)")
    print(f" - Target Sampling Ratio: {ratio * 100:.2f}%")
    print(f" - Random Seed: {seed}")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    total_data_rows = 0
    sampled_rows = 0

    with open(input_file, mode="r", encoding=encoding, errors="replace", newline="") as infile, \
         open(output_file, mode="w", encoding=encoding, errors="replace", newline="") as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        try:
            header = next(reader)
            writer.writerow(header)
        except StopIteration:
            raise ValueError(f"Input file {input_file} is empty!")

        for row in reader:
            if not row:
                continue

            total_data_rows += 1
            if random.random() < ratio:
                writer.writerow(row)
                sampled_rows += 1

            if total_data_rows % 100_000 == 0:
                print(f"   ... Streamed {total_data_rows:,} rows, sampled {sampled_rows:,} rows")

    elapsed_time = time.time() - start_time
    output_size_mb = output_file.stat().st_size / (1024 * 1024)
    throughput = total_data_rows / elapsed_time if elapsed_time > 0 else 0

    print(f"\n[Sampler Completed Successfully]")
    print(f" - Total Input Rows Processed: {total_data_rows:,}")
    print(f" - Sample Rows Written:        {sampled_rows:,} ({(sampled_rows/total_data_rows)*100:.2f}%)")
    print(f" - Output File:                {output_file} ({output_size_mb:.3f} MB)")
    print(f" - Elapsed Time:               {elapsed_time:.2f}s ({throughput:,.1f} rows/sec)\n")

    return {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "input_size_mb": round(input_size_mb, 2),
        "output_size_mb": round(output_size_mb, 3),
        "total_input_rows": total_data_rows,
        "sample_rows": sampled_rows,
        "elapsed_seconds": round(elapsed_time, 2),
        "throughput_rows_per_sec": round(throughput, 1)
    }


def extract_sample_head(
    input_file: Path,
    output_file: Path,
    num_rows: int = 5000,
    encoding: str = "utf-8"
) -> dict:
    """
    Fast sequential extraction of the first `num_rows` records.
    Completes in milliseconds without scanning the full file.
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    start_time = time.time()
    input_size_mb = input_file.stat().st_size / (1024 * 1024)
    print(f"\n[Sampler] Starting Fast Head Extraction...")
    print(f" - Input File:  {input_file} ({input_size_mb:.2f} MB)")
    print(f" - Target Head Rows: {num_rows:,}")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    count = 0

    with open(input_file, mode="r", encoding=encoding, errors="replace", newline="") as infile, \
         open(output_file, mode="w", encoding=encoding, errors="replace", newline="") as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        try:
            header = next(reader)
            writer.writerow(header)
        except StopIteration:
            raise ValueError(f"Input file {input_file} is empty!")

        for row in reader:
            if not row:
                continue
            writer.writerow(row)
            count += 1
            if count >= num_rows:
                break

    elapsed_time = time.time() - start_time
    output_size_mb = output_file.stat().st_size / (1024 * 1024)
    throughput = count / elapsed_time if elapsed_time > 0 else 0

    print(f"\n[Head Sampler Completed]")
    print(f" - Rows Extracted: {count:,}")
    print(f" - Output File:    {output_file} ({output_size_mb:.3f} MB)")
    print(f" - Elapsed Time:   {elapsed_time:.2f}s ({throughput:,.1f} rows/sec)\n")

    return {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "sample_rows": count,
        "elapsed_seconds": round(elapsed_time, 2),
        "output_size_mb": round(output_size_mb, 3)
    }


def parse_arguments() -> argparse.Namespace:
    """Configures CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Memory-Efficient CSV Sampler without Pandas (ELT Big Data Pipeline)"
    )

    # Determine default input path (check H: drive first, then local data dir)
    huge_file_h = Path("H:/midterm-data-pipeline/data/orders_huge_mixed_quality.csv")
    local_huge_file = DATA_DIR / "orders_huge_mixed_quality.csv"
    default_input = str(huge_file_h) if huge_file_h.exists() else str(local_huge_file)

    # Allow passing row count directly as positional argument (e.g. python src/create_small_sample.py 50000)
    parser.add_argument(
        "positional_rows",
        nargs="?",
        type=int,
        default=None,
        help="Optional positional row count (e.g. 50000)."
    )

    parser.add_argument(
        "-i", "--input",
        type=str,
        default=default_input,
        help="Path to the input large CSV file (default: orders_huge_mixed_quality.csv)."
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default=str(DATA_DIR / "orders_sample.csv"),
        help="Path to save the generated sample CSV file."
    )
    parser.add_argument(
        "-n", "--sample-size", "--rows",
        dest="sample_size",
        type=int,
        default=10000,
        help="Exact number of rows to sample (default: 10000)."
    )
    parser.add_argument(
        "-r", "--ratio",
        type=float,
        default=None,
        help="Sampling ratio (e.g. 0.05 for 5%%). If provided, overrides --sample-size."
    )
    parser.add_argument(
        "--head",
        action="store_true",
        help="Extract the first N rows sequentially (fastest option, instant)."
    )
    parser.add_argument(
        "-s", "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible results (default: 42)."
    )
    parser.add_argument(
        "--encoding",
        type=str,
        default="utf-8",
        help="File encoding (default: utf-8)."
    )

    return parser.parse_args()


def main():
    """Main CLI entrypoint."""
    args = parse_arguments()
    input_path = Path(args.input)
    output_path = Path(args.output)

    # If the user didn't specify a row count in the command, ask them interactively
    if args.positional_rows is None and "--rows" not in sys.argv and "-n" not in sys.argv and "--sample-size" not in sys.argv:
        try:
            user_input = input("\n👉 كم عدد السجلات (الصفوف) التي تريد سحبها في العينة؟ (مثال: 5000 أو 50000): ").strip()
            if user_input:
                target_rows = int(user_input)
            else:
                target_rows = 10000
        except (ValueError, EOFError, KeyboardInterrupt):
            target_rows = 10000
    else:
        target_rows = args.positional_rows if args.positional_rows is not None else args.sample_size

    if args.head or args.positional_rows is not None or target_rows is not None:
        extract_sample_head(
            input_file=input_path,
            output_file=output_path,
            num_rows=target_rows,
            encoding=args.encoding
        )
    elif args.ratio is not None:
        extract_sample_ratio(
            input_file=input_path,
            output_file=output_path,
            ratio=args.ratio,
            seed=args.seed,
            encoding=args.encoding
        )
    else:
        extract_sample_reservoir(
            input_file=input_path,
            output_file=output_path,
            sample_size=target_rows,
            seed=args.seed,
            encoding=args.encoding
        )


if __name__ == "__main__":
    main()
