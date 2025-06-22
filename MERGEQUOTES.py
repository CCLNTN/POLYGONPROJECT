import duckdb
import time
import os
from pathlib import Path
from datetime import datetime

# --- CONFIGURATION SECTION ---

# 1. Define the base directory where your monthly QUOTES Parquet folders are located.
BASE_PARQUET_DIR = r"C:\2025\QUOTES\PARQUET"

# --- END OF CONFIGURATION ---

def is_valid_daily_file(file_path: Path) -> bool:
    """
    Checks if a file is a valid daily parquet file (e.g., '2025-06-09_quotes.parquet').
    """
    # We check if the filename starts with a date format.
    try:
        # Tries to parse the first 10 characters of the filename.
        datetime.strptime(file_path.name[:10], '%Y-%m-%d')
        return True
    except ValueError:
        return False

def merge_files_in_folder(month_folder: Path):
    """
    Intelligently merges only new daily files into the monthly master file.
    """
    year = month_folder.parent.parent.name
    month = month_folder.name
    merged_file_name = f"{year}-{month}-Merged_quotes.parquet"
    merged_file_path = month_folder / merged_file_name

    # 1. Get a list of all valid daily .parquet files in the folder.
    all_daily_files = {f for f in month_folder.glob('*.parquet') if is_valid_daily_file(f)}

    if not all_daily_files:
        print("    > No valid daily parquet files found to merge.")
        return

    # 2. Intelligently determine which files need to be processed.
    files_to_process = set()
    if not merged_file_path.exists():
        print("    > Merged file does not exist. Processing all daily files.")
        files_to_process = all_daily_files
    else:
        # Find the last date we have already processed.
        try:
            # This query efficiently gets the latest date from the existing merged file.
            latest_date_in_merged = duckdb.sql(f"SELECT MAX(participant_timestamp) FROM '{merged_file_path.as_posix()}'").fetchone()[0]
            print(f"    > Latest timestamp in merged file: {latest_date_in_merged.strftime('%Y-%m-%d %H:%M:%S') if latest_date_in_merged else 'None'}")
            # Find any daily files that are newer than the latest data in the merged file.
            for f in all_daily_files:
                file_date_str = f.name[:10]
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d').date()
                if not latest_date_in_merged or file_date > latest_date_in_merged.date():
                    files_to_process.add(f)

        except Exception as e:
            print(f"    > Could not read existing merged file, will rebuild it. Reason: {e}")
            files_to_process = all_daily_files # If we can't read it, rebuild from scratch

    if not files_to_process:
        print("    > Merged file is already up-to-date. No new daily files found.")
        return

    # 3. Perform the merge.
    # We will combine the existing data (if any) with the new data.
    files_for_union = {merged_file_path} | files_to_process if merged_file_path.exists() else files_to_process
    
    # Filter out non-existent paths just in case
    files_for_union = {p for p in files_for_union if p.exists()}
    
    temp_merged_file = month_folder / f"{merged_file_name}.tmp"
    file_paths_for_query = [f"'{p.as_posix()}'" for p in files_for_union]

    print(f"    > Merging {len(files_to_process)} new file(s) with existing data...")

    try:
        start_time = time.perf_counter()
        # The query now reads from a combined list of the old merged file and new daily files.
        query = f"""
            COPY (
                SELECT * FROM read_parquet([{",".join(file_paths_for_query)}])
            )
            TO '{temp_merged_file.as_posix()}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
        """
        duckdb.sql(query)
        end_time = time.perf_counter()
        
        # Atomically replace the old file with the new one.
        if merged_file_path.exists():
            merged_file_path.unlink()
        temp_merged_file.rename(merged_file_path)
        
        duration = end_time - start_time
        print(f"    > SUCCESS: Updated merged file in {duration:.2f}s")

    except Exception as e:
        print(f"    > FAILED: {e}")
        if temp_merged_file.exists():
            temp_merged_file.unlink()

# --- Main execution block ---
if __name__ == "__main__":
    print("Starting incremental merge process for QUOTES...")
    print(f"Scanning directory: '{BASE_PARQUET_DIR}'")
    
    base_path = Path(BASE_PARQUET_DIR)
    
    if not base_path.exists():
        print(f"\nError: The Parquet directory '{base_path}' was not found.")
        exit()

    month_folders = sorted([d for d in base_path.iterdir() if d.is_dir()], key=lambda d: d.name)
    
    if not month_folders:
        print("\nError: No month folders found to process.")
        exit()

    total_start_time = time.perf_counter()
    
    for month_folder in month_folders:
        print(f"\n--- Checking Month: {month_folder.name} ---")
        merge_files_in_folder(month_folder)

    total_end_time = time.perf_counter()
    total_duration = total_end_time - total_start_time

    print("\n-------------------------------------------")
    print("Incremental Merge Summary")
    print("-------------------------------------------")
    print(f"Checked {len(month_folders)} month folder(s).")
    print(f"Total time elapsed: {total_duration:.2f} seconds")
    print("-------------------------------------------")