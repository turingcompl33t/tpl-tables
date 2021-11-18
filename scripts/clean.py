# clean.py
# Clean TPC tables for DBMS loading.

import os
import sys
import argparse
import subprocess
import pandas as pd

# Script exit codes
EXIT_SUCCESS = 0
EXIT_FAILURE = 1


# -----------------------------------------------------------------------------
# Logging


def info(msg: str):
    """Log an information message."""
    print(f"[+] {msg}")


def error(msg: str):
    """Log an error message."""
    print(f"[-] {msg}")


# -----------------------------------------------------------------------------
# Argument Parsing


def parse_arguments():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "tables_path",
        help="The path to the directory in which tables reside.",
        type=str,
    )
    args = parser.parse_args()
    return args.tables_path


# -----------------------------------------------------------------------------
# General FS Helpers


def is_raw_table_file(filename: str) -> bool:
    """Determine if a filename represents a raw table file."""
    return filename.endswith(".tbl")


def filename(fullname: str) -> str:
    """ "Extract the filename from filename with extension."""
    return os.path.splitext(fullname)[0]


# -----------------------------------------------------------------------------
# Table Preprocessing


def strip_trailing_pipes(path: str):
    """
    Strip the trailing pipe from all lines in raw table file.
    :param path The path to the input file
    """
    p = subprocess.run(["sed", "-i", "s/[|]$//", path])
    if p.returncode != EXIT_SUCCESS:
        raise RuntimeError("Failed to replace pipes.")


def preprocess_table(tables_path: str, table_name: str):
    """
    Preprocess the table identified by `table_name`.
    :param tables_path The path to which tables are saved
    :param table_name The name of the table to preprocess
    """
    info(f"Preprocessing {table_name}...")

    path = os.path.join(tables_path, f"{table_name}.tbl")
    strip_trailing_pipes(path)

    info("Done.")


def preprocess_tables(tables_path: str):
    """
    Preprocess all of the tables present in the directory at `tables_path`.
    :param tables_path The path to the directory in which input tables reside
    """
    for table in filter(is_raw_table_file, os.listdir(tables_path)):
        preprocess_table(tables_path, filename(table))


# -----------------------------------------------------------------------------
# Table Cleaning


def clean_table(tables_path: str, table_name: str):
    """ "
    Clean the table identified by `table_name`.
    :param tables_path The path to which tables are saved
    :param table_name The name of the table to clean
    """
    info(f"Cleaning table {table_name}...")

    ipath = f"{os.path.join(tables_path, table_name)}.tbl"
    opath = f"{os.path.join(tables_path, table_name)}.data"

    df = pd.read_csv(ipath, sep="|", header=None, index_col=False, encoding="latin-1")
    df.to_csv(opath, index=False)

    info("Done.")


def clean_tables(tables_path: str):
    """
    Clean all of the tables present in the directory at `tables_path`.
    :param tables_path The path to the directory in which input tables reside
    """
    for table in filter(is_raw_table_file, os.listdir(tables_path)):
        clean_table(tables_path, filename(table))


# -----------------------------------------------------------------------------
# Main


def main() -> int:
    tables_path = parse_arguments()

    try:
        # Preprocess tables
        preprocess_tables(tables_path)
        # Clean tables
        clean_tables(tables_path)
    except RuntimeError as e:
        error(f"{e}")
        return EXIT_FAILURE

    return EXIT_SUCCESS


# -----------------------------------------------------------------------------
# Script Entry

if __name__ == "__main__":
    sys.exit(main())
