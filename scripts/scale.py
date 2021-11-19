# scale.py
# Take a subset of raw data table files based on scale factor.

import os
import sys
import argparse
import subprocess

# Script exit codes
EXIT_SUCCESS = 0
EXIT_FAILURE = 1

# -----------------------------------------------------------------------------
# Logging


def info(msg: str):
    """Log an informational message."""
    print(f"[+] {msg}")


def error(msg: str):
    """Log an error message."""
    print(f"[-] {msg}")


# -----------------------------------------------------------------------------
# Argument Parsing


def parse_arguments():
    """
    Parse commandline arguments.
    :return The parsed arguments object
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path",
        help="The path to the directory in which the tables are stored.",
        type=str,
    )
    parser.add_argument("scale_factor", help="The scale factor [0, 1.0]", type=float)
    args = parser.parse_args()
    return args.path, args.scale_factor


# -----------------------------------------------------------------------------
# Table Scaling


def is_raw_table_file(filename: str) -> bool:
    """Determine if a file is a raw table file."""
    return os.path.splitext(filename)[1] == ".tbl"


def line_count(path: str) -> int:
    """
    Return the number of lines in the file at `path`.
    :param path The path to the file of interest
    :return The number of lines in the file
    """
    return sum(1 for _ in open(path, "r", encoding="latin-1"))


def head(path: str, n: int):
    """
    Truncate the file at `path` to the first `n` lines.
    :param path The path to the file
    :param n The number of lines to which file is truncated
    """
    # Take the head of the input file
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="latin-1") as f:
        p = subprocess.run(["head", "-n", f"{n}", path], stdout=f)
        if p.returncode != EXIT_SUCCESS:
            raise RuntimeError(f"Failed to scale table at {path} ({p.returncode})")

    # Replace the input file with the new file
    p = subprocess.run(["mv", "-f", tmp_path, path])
    if p.returncode != EXIT_SUCCESS:
        raise RuntimeError(f"Failed to scale table at {path} ({p.returncode})")


def scale_table_at(path: str, sf: float):
    """
    Scale an individual table at `path`.
    :param path The path to the table
    :param sf The scale factor
    """
    # Compute the number of lines to which file is scaled
    n = max(1, int(line_count(path) * sf))

    info(f"Scaling table at {path} to {n} rows...")

    head(path, n)

    info("Done.")


def scale_tables_at(path: str, sf: float):
    """
    Scale all tables at `path`.
    :param path The path to the directory at which tables are stored
    :param sf The scale factor
    """
    for table_file in filter(is_raw_table_file, os.listdir(path)):
        scale_table_at(os.path.join(path, table_file), sf)


# -----------------------------------------------------------------------------
# Main


def main() -> int:
    path, sf = parse_arguments()
    try:
        scale_tables_at(path, sf)
    except RuntimeError as e:
        error(f"{e}")
        return EXIT_FAILURE

    return EXIT_SUCCESS


# -----------------------------------------------------------------------------
# Script Entry

if __name__ == "__main__":
    sys.exit(main())
