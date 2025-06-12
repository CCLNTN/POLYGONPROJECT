import duckdb
import time
import os
import sys

def benchmark_duckdb_conversion(drive_path, drive_type):
    """
    Uses DuckDB to convert a compressed CSV to a Parquet file on the specified path
    and logs the total time taken for the operation.
    """
    # Assuming the same quotes file name as before
    source_file = os.path.join(drive_path, "2025-06-09.csv.gz")
    destination_file = os.path.join(drive_path, "2025-06-09.duckdb.parquet") # New name to avoid overwriting

    print(f"--- Starting DuckDB Benchmark for {drive_type} ({drive_path}) ---")

    if not os.path.exists(source_file):
        print(f"Error: Source file not found at '{source_file}'")
        print(f"--- FAILED Benchmark for {drive_type} ---\n")
        return

    try:
        # Start the timer
        start_time = time.perf_counter()

        # DuckDB's engine is built in C++ and highly optimized for this exact task.
        # read_csv_auto handles decompression, type detection, and parsing.
        # The COPY command streams the result directly to a Parquet file.
        # This one command replaces the entire read/write process from Pandas.
        duckdb.sql(f"""
            COPY (
                SELECT * FROM read_csv_auto('{source_file}')
            )
            TO '{destination_file}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
        """)

        # Stop the timer
        end_time = time.perf_counter()
        duration = end_time - start_time
        
        print(f"\nSUCCESS: DuckDB conversion complete for {drive_type}.")
        print(f"Total time elapsed: {duration:.4f} seconds.")

    except Exception as e:
        print(f"An error occurred during the DuckDB benchmark: {e}")
    finally:
        print(f"--- Finished DuckDB Benchmark for {drive_type} ---\n")

# --- Main execution block ---
if __name__ == "__main__":
    # --- CONFIGURATION for TRADES ---
    # Same paths as before for your TRADES files.
    drive_paths = {
        "NVMe": r"C:\2025\TRADES\GZFILES\06",
        "SSD":  r"S:\2025\TRADES\GZFILES\06",
        "HDD":  r"Z:\2025\TRADES\GZFILES\06"
    }

    if len(sys.argv) != 2 or sys.argv[1] not in drive_paths:
        print("Usage: python BenchmarkDuckDB.py [DRIVE_TYPE]")
        print("Available DRIVE_TYPE options:", list(drive_paths.keys()))
        sys.exit(1)
        
    drive_to_test = sys.argv[1]
    path_to_test = drive_paths[drive_to_test]
    
    benchmark_duckdb_conversion(path_to_test, drive_to_test)