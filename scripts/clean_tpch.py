# clean_tpch.py
# Clean TPCH tables.

import sys
import argparse
import pandas as pd

EXIT_SUCCESS = 0
EXIT_FAILURE = 1


def info(msg: str):
    """Log an information message."""
    print(f"[+] {msg}")


def error(msg: str):
    """Log an error message."""
    print(f"[-] {msg}")


def parse_arguments():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "table_name", help="The name of the table to generate.", type=str
    )
    args = parser.parse_args()
    return args.table_name


def generate_csv(table_name: str):
    """
    Generate the CSV for the given table.
    :param table_name The name of the table
    """
    info(f"Cleaning up CSV file for table {table_name}...")

    df = pd.read_csv(f"{table_name}.tbl", sep="|", header=None, index_col=False)
    df.to_csv(f"{table_name}.data", index=False)

    info("Done.")


def main() -> int:
    table_name = parse_arguments()
    generate_csv(table_name)
    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
