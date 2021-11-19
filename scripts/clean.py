# clean.py
# Clean TPC tables for DBMS loading.

import os
import sys
import argparse
import subprocess
import pandas as pd
from enum import Enum
from typing import List

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
# Schema Loading


class ColumnType(Enum):
    INTEGER = 0
    FLOAT8 = 1
    CHAR = 2
    VARCHAR = 3
    DATE = 4

    def from_string(s: str):
        if s == "integer":
            return ColumnType.INTEGER
        if s == "float8":
            return ColumnType.FLOAT8
        if s == "char":
            return ColumnType.CHAR
        if s == "varchar":
            return ColumnType.VARCHAR
        if s == "date":
            return ColumnType.DATE
        raise RuntimeError("Invalid column type string.")

    def to_string(type) -> str:
        if type == ColumnType.INTEGER:
            return "INTEGER"
        if type == ColumnType.FLOAT8:
            return "FLOAT8"
        if type == ColumnType.CHAR:
            return "CHAR"
        if type == ColumnType.VARCHAR:
            return "VARCHAR"
        if type == ColumnType.DATE:
            return "DATE"
        raise RuntimeError("Invalid column type.")


class Column:
    """The Column class represents a single column in a schema."""

    @staticmethod
    def from_line(line: str):
        """
        Parse a column from a line.
        :param line The line of text
        :return The Column
        """
        s = line.strip().split()
        assert len(s) >= 2, "Malformed column."
        return Column(s[0], ColumnType.from_string(s[1]))

    def __init__(self, name: str, type: ColumnType):
        """ "
        Initialize a new Column instance.
        :param name The name of the column
        :param type The type of the column
        """
        self.name = name
        self.type = type

    def __str__(self):
        """Return a string representation of the column."""
        return f"{self.name} {ColumnType.to_string(self.type)}"


class Schema:
    """The Schema class represents a SQL schema."""

    @staticmethod
    def from_file(path: str):
        """
        Construct a Schema from an input file.
        :param path The path to the file
        :return The Schema
        """
        name = None
        columns = []
        with open(path, "r") as f:
            header = f.readline()
            name = header.strip().split()[0]
            for line in f:
                # Break when we hit index information
                if len(line.strip()) == 0:
                    break
                columns.append(Column.from_line(line))
        return Schema(name, columns)

    def __init__(self, name: str, columns: List[Column]):
        """
        Initialize a new Schema instance.
        :param name The schema name
        :param columns The list of columns
        """
        self.name = name
        self.columns = columns

    def __str__(self) -> str:
        """Return a string representation of the schema."""
        # Compute the maximum length column name
        m = max([len(column.name) for column in self.columns])

        s = ""
        s += self.name
        s += " (\n"
        for column in self.columns:
            s += f"\t{str.ljust(column.name, m + 1)} {ColumnType.to_string(column.type)}\n"
        s += ")"
        return s


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
    spath = f"{os.path.join(tables_path, table_name)}.schema"

    # Load the schema file for the table
    schema = Schema.from_file(spath)

    df = pd.read_csv(ipath, sep="|", header=None, index_col=False, encoding="latin-1")
    # Manually convert floating point columns back to integers
    for col_idx, col in enumerate(schema.columns):
        if col.type == ColumnType.INTEGER:
            df.iloc[:, col_idx] = df.iloc[:, col_idx].fillna(0)
            df.iloc[:, col_idx] = df.iloc[:, col_idx].astype(int)

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
