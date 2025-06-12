import duckdb
import time
import os
import sys

# --- CONFIGURATION SECTION ---

COLUMN_DEFINITIONS = {
    'ticker': 'VARCHAR',
    'conditions': 'VARCHAR',
    'correction': 'BIGINT',
    'exchange': 'BIGINT',
    'id': 'VARCHAR',
    'participant_timestamp': 'TIMESTAMP_US',
    'price': 'DOUBLE',
    'sequence_number': 'BIGINT',
    'sip_timestamp': 'TIMESTAMP_US',
    'size': 'BIGINT',
    'tape': 'BIGINT',
    'trf_id': 'VARCHAR',
    'trf_timestamp': 'TIMESTAMP_US'
}

TICKER_COLUMN_NAME = 'ticker'

FILTER_CONFIGS = {
    'include10': {
        'file': r'C:\CODE_REPOSITORY\_prod\10SYMBOLS.txt',
        'operator': 'IN'
    },
    'include50': {
        'file': r'C:\CODE_REPOSITORY\_prod\50SYMBOLS.txt',
        'operator': 'IN'
    },
    'excludeETFs': {
        'file': r'C:\CODE_REPOSITORY\_prod\1ALLSYMBOLSNOETFS.txt',
        'operator': 'NOT IN'
    }
}

DRIVE_PATHS = {
    "NVMe": r"C:\2025\TRADES\GZFILES\06",
    "SSD": r"S:\2025\TRADES\GZFILES\06",
    "HDD": r"Z:\2025\TRADES\GZFILES\06"
}
# --- END OF CONFIGURATION ---

def benchmark_with_filter(drive_path, drive_type, filter_name, filter_config):
    source_file = os.path.join(drive_path, "2025-06-09.csv.gz")
    destination_file = os.path.join(drive_path, f"2025-06-09_trades_filtered_typed_{filter_name}.parquet")
    
    filter_file = filter_config['file']
    filter_operator = filter_config['operator']

    select_clause_parts = []
    for name, dtype in COLUMN_DEFINITIONS.items():
        if dtype == 'TIMESTAMP_US':
            select_clause_parts.append(
                f"make_timestamp((CAST(\"{name}\" AS HUGEINT) / 1000)::BIGINT) AS \"{name}\""
            )
        else:
            select_clause_parts.append(f"\"{name}\"")
    select_clause = ", ".join(select_clause_parts)

    print(f"--- Starting Typed Trades Benchmark for {drive_type} (Filter: {filter_name}) ---")
    print(f"Applying types (timestamps as microsecond) and filtering on '{TICKER_COLUMN_NAME}'...")

    for f in [source_file, filter_file]:
        if not os.path.exists(f):
            print(f"Error: Required file not found at '{f}'")
            return

    types_struct = "{" + ",".join([f"'{k}':'{v}'" for k, v in COLUMN_DEFINITIONS.items()]) + "}"

    try:
        start_time = time.perf_counter()

        query = f"""
            COPY (
                SELECT {select_clause}
                FROM (
                    SELECT *
                    FROM read_csv(
                        '{source_file}',
                        header=True,
                        auto_detect=False,
                        ignore_errors=True,
                        columns={types_struct}
                    )
                ) AS raw_data
                WHERE "{TICKER_COLUMN_NAME}" {filter_operator}
                      (SELECT * FROM read_csv_auto('{filter_file}'))
            )
            TO '{destination_file}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
        """

        print("Executing DuckDB query with type casting...")
        duckdb.sql(query)
        print("Query finished.")

        end_time = time.perf_counter()
        duration = end_time - start_time
        
        print(f"\nSUCCESS: Typed & filtered conversion complete for {drive_type} using '{filter_name}'.")
        print(f"Total time elapsed: {duration:.4f} seconds.")

    except Exception as e:
        print(f"An error occurred during the DuckDB benchmark: {e}")
    finally:
        print(f"--- Finished Benchmark for {drive_type} (Filter: {filter_name}) ---\n")

if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] not in DRIVE_PATHS or sys.argv[2] not in FILTER_CONFIGS:
        print("Usage: python FilteredSymbolTradeBenchMarking.py [DRIVE_TYPE] [FILTER_NAME]")
        print("Available DRIVE_TYPE options:", list(DRIVE_PATHS.keys()))
        print("Available FILTER_NAME options:", list(FILTER_CONFIGS.keys()))
        sys.exit(1)
        
    drive_to_test = sys.argv[1]
    filter_to_test = sys.argv[2]

    path_to_test = DRIVE_PATHS[drive_to_test]
    config_to_test = FILTER_CONFIGS[filter_to_test]
    
    benchmark_with_filter(path_to_test, drive_to_test, filter_to_test, config_to_test)