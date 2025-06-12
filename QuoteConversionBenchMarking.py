import pandas as pd
import time
import os
import sys

def benchmark_conversion(drive_path, drive_type):
    """
    Reads a compressed CSV from a specified path, converts it to Parquet in the same path,
    and logs the time taken for reading, writing, and the total operation.
    """
    # NOTE: Assuming the file is now a quotes file. Update the name if needed.
    source_file = os.path.join(drive_path, "2025-06-09.csv.gz")
    destination_file = os.path.join(drive_path, "2025-06-09.parquet")

    print(f"--- Starting Benchmark for {drive_type} ({drive_path}) ---")

    if not os.path.exists(source_file):
        print(f"Error: Source file not found at '{source_file}'")
        print(f"--- FAILED Benchmark for {drive_type} ---\n")
        return

    try:
        total_start_time = time.perf_counter()

        read_start_time = time.perf_counter()

        # --- THIS IS THE CORRECTED LINE ---
        # Force columns 7 and 8 to be read as strings to prevent mixed-type errors
        df = pd.read_csv(source_file, compression="gzip", dtype={7: str, 8: str})
        
        read_end_time = time.perf_counter()
        read_duration = read_end_time - read_start_time
        print(f"Step 1: Reading compressed CSV took {read_duration:.4f} seconds.")

        write_start_time = time.perf_counter()
        df.to_parquet(destination_file, engine="pyarrow", compression="snappy")
        write_end_time = time.perf_counter()
        write_duration = write_end_time - write_start_time
        print(f"Step 2: Writing Parquet file took  {write_duration:.4f} seconds.")

        total_end_time = time.perf_counter()
        total_duration = total_end_time - total_start_time
        
        print(f"\nSUCCESS: Conversion complete for {drive_type}.")
        print(f"Total time elapsed: {total_duration:.4f} seconds.")

    except Exception as e:
        print(f"An error occurred during the benchmark: {e}")
    finally:
        print(f"--- Finished Benchmark for {drive_type} ---\n")

# --- Main execution block ---
if __name__ == "__main__":
    # --- CONFIGURATION for QUOTES ---
    # Update these paths to the location of your QUOTES files.
    drive_paths = {
        "NVMe": r"C:\2025\QUOTES\GZFILES\06",
        "SSD":  r"S:\2025\QUOTES\GZFILES\06",
        "HDD":  r"Z:\2025\QUOTES\GZFILES\06"
    }

    if len(sys.argv) != 2 or sys.argv[1] not in drive_paths:
        print("Usage: python your_script_name.py [DRIVE_TYPE]")
        print("Available DRIVE_TYPE options:", list(drive_paths.keys()))
        sys.exit(1)
        
    drive_to_test = sys.argv[1]
    path_to_test = drive_paths[drive_to_test]
    
    benchmark_conversion(path_to_test, drive_to_test)