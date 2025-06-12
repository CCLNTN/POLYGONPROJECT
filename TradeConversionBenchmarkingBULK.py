import duckdb
import time
import os
from pathlib import Path

# --- CONFIGURATION SECTION ---

# 1. Define the base directories for input and output.
BASE_INPUT_DIR = r"Z:\2025\TRADES\GZFILES"
BASE_OUTPUT_DIR = r"S:\2025\TRADES\PARQUET"

# 2. Define the explicit schema for the source TRADES CSV files.
COLUMN_DEFINITIONS = {
    'ticker': 'VARCHAR',
    'conditions': 'VARCHAR',
    'correction': 'BIGINT',
    'exchange': 'BIGINT',
    'id': 'VARCHAR',
    'participant_timestamp': 'VARCHAR',
    'price': 'DOUBLE',
    'sequence_number': 'BIGINT',
    'sip_timestamp': 'VARCHAR',
    'size': 'BIGINT',
    'tape': 'BIGINT',
    'trf_id': 'VARCHAR',
    'trf_timestamp': 'VARCHAR'
}

# 3. Define the filter file to be used for this bulk run.
FILTER_FILE = r'C:\CODE_REPOSITORY\_prod\10SYMBOLS.txt'
TICKER_COLUMN_NAME = 'ticker'

# --- END OF CONFIGURATION ---

def convert_file(source_path: Path, output_dir: Path):
    """
    Uses DuckDB to convert a single trades CSV to a typed, filtered Parquet file.
    """
    destination_file = output_dir / f"{source_path.stem.replace('.csv', '')}.parquet"
    
    select_clause_parts = []
    for name, dtype in COLUMN_DEFINITIONS.items():
        if 'timestamp' in name:
            select_clause_parts.append(
                f"make_timestamp((CAST(\"{name}\" AS HUGEINT) / 1000)::BIGINT) AS \"{name}\""
            )
        else:
            select_clause_parts.append(f"\"{name}\"")
    select_clause = ", ".join(select_clause_parts)

    types_struct = "{" + ",".join([f"'{k}':'{v}'" for k, v in COLUMN_DEFINITIONS.items()]) + "}"

    try:
        start_time = time.perf_counter()
        query = f"""
            COPY (
                SELECT {select_clause}
                FROM (
                    SELECT *
                    FROM read_csv(
                        '{source_path.as_posix()}',
                        header=True,
                        auto_detect=False,
                        ignore_errors=True,
                        columns={types_struct}
                    )
                ) AS raw_data
                WHERE "{TICKER_COLUMN_NAME}" IN (SELECT * FROM read_csv_auto('{Path(FILTER_FILE).as_posix()}'))
            )
            TO '{destination_file.as_posix()}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
        """
        duckdb.sql(query)
        end_time = time.perf_counter()
        
        duration = end_time - start_time
        print(f"    > SUCCESS: Converted in {duration:.2f}s")
        return True

    except Exception as e:
        print(f"    > FAILED: {e}")
        return False

# --- Main execution block ---
if __name__ == "__main__":
    print(f"Starting bulk conversion...")
    print(f"Input source:  '{BASE_INPUT_DIR}'")
    print(f"Output target: '{BASE_OUTPUT_DIR}'")
    print(f"Filter list:   '{FILTER_FILE}'")

    base_path = Path(BASE_INPUT_DIR)

    # --- MODIFIED LOGIC ---
    # 1. Get all subdirectories (e.g., '01', '02', ...) in the base directory.
    try:
        month_folders = [d for d in base_path.iterdir() if d.is_dir()]
    except FileNotFoundError:
        print(f"\nError: The input directory '{base_path}' was not found. Please check the BASE_INPUT_DIR path.")
        exit()

    # 2. Sort the folders by name in reverse order ('12', '11', ... '01').
    sorted_folders = sorted(month_folders, key=lambda d: d.name, reverse=True)

    if not sorted_folders:
        print(f"\nError: No month folders found in '{base_path}'.")
        exit()
        
    print(f"\nFound {len(sorted_folders)} month folders to process in reverse order.")
    
    total_start_time = time.perf_counter()
    total_files_processed = 0
    success_count = 0
    fail_count = 0

    # 3. Loop through each folder from most recent to oldest.
    for month_folder in sorted_folders:
        print(f"\n--- Processing Month: {month_folder.name} ---")
        
        source_files_in_month = list(month_folder.glob('*.csv.gz'))
        
        if not source_files_in_month:
            print("  No '.csv.gz' files found in this folder.")
            continue
            
        print(f"  Found {len(source_files_in_month)} files.")

        for source_file in source_files_in_month:
            total_files_processed += 1
            print(f"  [{total_files_processed}] Processing file: {source_file.name}")
            
            # Determine the correct output subdirectory
            relative_path = source_file.relative_to(base_path)
            output_sub_dir = Path(BASE_OUTPUT_DIR) / relative_path.parent
            
            # Create the output directory if it doesn't exist
            output_sub_dir.mkdir(parents=True, exist_ok=True)
            
            if convert_file(source_file, output_sub_dir):
                success_count += 1
            else:
                fail_count += 1
            
    total_end_time = time.perf_counter()
    total_duration = total_end_time - total_start_time

    print("\n-------------------------------------------")
    print("Bulk Conversion Summary")
    print("-------------------------------------------")
    print(f"Successfully converted: {success_count} files")
    print(f"Failed to convert:      {fail_count} files")
    print(f"Total time elapsed:     {total_duration / 60:.2f} minutes")
    print("-------------------------------------------")