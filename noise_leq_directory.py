#!/usr/bin/env python3
"""
laeq_folder.py ― Compute sequential L(A/C)eq values from 1-second records
                 from all files in a specified folder.

Usage
-----
    python laeq_folder.py path/to/your_folder [--include-short-periods] [--csv]

Arguments:
  path/to/your_folder       : Input folder with 1s dBA/dBC sample files.
  --include-short-periods : If specified, output includes periods shorter
                              than 15 minutes. Otherwise, only full 15-min
                              periods are output.
  --csv                     : Emit one CSV line per period.


Data format expected per file
-----------------------------
Each file must start with the header line produced by the logger, e.g.

    STANDARD Sound Level Meter DATA LOGGER SamplingRate:1.0;

and every following data line must look like:

    DD-MM-YYYY,HH:MM:SS, <level>, <unit>

Example:

    25-05-2025,20:20:34, 52.30, dBA
    31-05-2025,15:45:49, 81.10, dBC
"""

import argparse
import csv as csv_parser # Renamed to avoid conflict with args.csv
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

SECONDS_PER_BLOCK = 15 * 60  # 900 s → “Leq15”
# Updated FIELDNAMES for CSV output
FIELDNAMES = ["start", "end", "seconds", "Leq_value", "unit"]


def parse_file_samples(path: Path):
    """
    Yields (datetime, float level, str unit) samples from a single file.
    The 'unit' (e.g., "dBA", "dBC") is determined from the first valid data line
    and is expected to be consistent throughout the file.
    Rows with mismatching units or malformed data are skipped with a warning.
    """
    file_specific_unit = None
    try:
        with path.open(newline="", encoding="utf-8") as fp:
            reader = csv_parser.reader(fp)
            header_seen = False
            for line_num, row in enumerate(reader, 1):
                if not header_seen and row and "STANDARD" in row[0]:
                    header_seen = True
                    continue

                if len(row) < 4:  # Expect Date, Time, Level, Unit
                    if any(s.strip() for s in row): # Report if not a completely blank line
                        print(f"Warning: Skipping malformed/short row {line_num} in {path.name}: {row}", file=sys.stderr)
                    continue
                
                date_str, time_str, level_str, current_row_unit = [s.strip() for s in row[:4]]

                try:
                    ts = datetime.strptime(f"{date_str},{time_str}", "%d-%m-%Y,%H:%M:%S")
                    level = float(level_str)
                    
                    if file_specific_unit is None:
                        file_specific_unit = current_row_unit
                    elif file_specific_unit != current_row_unit:
                        print(f"Warning: Unit mismatch in {path.name} at line {line_num}. Expected {file_specific_unit}, got {current_row_unit}. Skipping row.", file=sys.stderr)
                        continue
                    
                    yield ts, level, file_specific_unit
                
                except ValueError:
                    print(f"Warning: Skipping invalid data in row {line_num} in {path.name}: {row}", file=sys.stderr)
                    continue
        
        if file_specific_unit is None and header_seen:
             print(f"Warning: No valid data lines found after header in {path.name}. File processed, but no samples yielded.", file=sys.stderr)
        elif not header_seen and file_specific_unit is None:
             print(f"Warning: No header or valid data lines found in {path.name}. File could not be processed.", file=sys.stderr)

    except FileNotFoundError:
        print(f"Error: File not found {path}", file=sys.stderr)
    except Exception as e:
        print(f"Error processing file {path.name}: {e}", file=sys.stderr)


def groups_of_seconds(samples):
    """
    Split the stream of (ts, level, unit) samples into sequential blocks of
    SECONDS_PER_BLOCK seconds. The very first timestamp defines time zero for
    the first block from that sample stream. Yields (start_dt, end_dt, levels_list, block_unit).
    """
    current_start_dt, levels, current_block_unit = None, [], None
    first_sample_in_stream = True

    for ts, lvl, sample_unit in samples:
        if first_sample_in_stream:
            current_start_dt = ts
            block_end_dt = current_start_dt + timedelta(seconds=SECONDS_PER_BLOCK)
            current_block_unit = sample_unit
            first_sample_in_stream = False

        while ts >= block_end_dt:
            if levels:
                yield current_start_dt, block_end_dt, levels, current_block_unit
            current_start_dt = block_end_dt
            block_end_dt = current_start_dt + timedelta(seconds=SECONDS_PER_BLOCK)
            levels = []
        levels.append(lvl)
    
    if levels and current_start_dt is not None:
        actual_end_dt = current_start_dt + timedelta(seconds=len(levels))
        yield current_start_dt, actual_end_dt, levels, current_block_unit


def leq(levels):
    """Return Leq for a list of sound-pressure levels (dBA, dBC, etc.)."""
    if not levels:
        return float("nan")
    try:
        linear_sum = sum(10 ** (L / 10.0) for L in levels)
        if linear_sum == 0:
             return float("-inf") if any(L == float("-inf") for L in levels) else float("nan")
        mean_linear = linear_sum / len(levels)
        return 10.0 * math.log10(mean_linear)
    except OverflowError:
        return float("inf")


def compute_for_file(path: Path):
    """
    Generator that yields dictionaries of Leq results for a single file.
    """
    for start, end, levels, unit_for_block in groups_of_seconds(parse_file_samples(path)):
        yield {
            "start": start,
            "end": end,
            "seconds": len(levels), # Number of 1-second samples in this block
            "Leq_value": round(leq(levels), 2),
            "unit": unit_for_block,
        }


def main():
    parser = argparse.ArgumentParser(
        description="Compute sequential L(A/C)eq15 values from files in a folder.",
        formatter_class=argparse.RawTextHelpFormatter # To preserve help text formatting
    )
    parser.add_argument("folder", type=Path, help="Input folder with 1s dBA/dBC sample files.")
    parser.add_argument("--include-short-periods", action="store_true",
                        help="If specified, output includes measurement periods shorter than 15 minutes.\n"
                             "Otherwise (default), only full 15-minute periods (900 seconds) are output.")
    parser.add_argument("--csv", action="store_true",
                        help="Output comma-separated values (machine friendly).")
    args = parser.parse_args()

    if not args.folder.is_dir():
        print(f"Error: Provided path '{args.folder}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    all_results = []
    for file_path in args.folder.iterdir():
        if file_path.is_file():
            for result_item in compute_for_file(file_path):
                all_results.append(result_item)
    
    if not all_results:
        print("No results generated. Check input files and folder.", file=sys.stderr)
        return

    all_results.sort(key=lambda x: x["start"])

    # Filter results based on --include-short-periods argument
    if args.include_short_periods:
        results_to_print = all_results
    else: # Default behavior: only include full 15-minute periods
        results_to_print = [res for res in all_results if res["seconds"] == SECONDS_PER_BLOCK]
    
    if not results_to_print and not args.include_short_periods and all_results:
        print("Note: No full 15-minute periods found. To include shorter periods, use the --include-short-periods flag.", file=sys.stderr)
    elif not results_to_print and not all_results: # Handles if all_results was empty to begin with
        pass # Message already printed above
    
    is_first_csv_line = True
    for row in results_to_print:
        if args.csv:
            if is_first_csv_line:
                # print(",".join(FIELDNAMES)) # Uncomment if you want a header in CSV output
                is_first_csv_line = False
            print(",".join(str(row[k]) for k in FIELDNAMES))
        else:
            s = row["start"].strftime("%Y-%m-%d %H:%M:%S")
            e = row["end"].strftime("%Y-%m-%d %H:%M:%S")
            dur = row["seconds"]
            val = row["Leq_value"]
            unit = row["unit"]
            print(f"{s} – {e}  ({dur:>4} s)  Leq = {val:>6.2f} {unit}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCalculation interrupted by user.", file=sys.stderr)
        sys.exit(130)
