# gen_schema.py
# Generate schema (.schema) files for data loading from SQL DDL.

import os
import re
import sys
import argparse
from typing import Tuple, List

EXIT_SUCCESS = 0
EXIT_FAILURE = 1

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------


def info(msg: str):
    """Log an informational message."""
    print(f"[+] {msg}")


def error(msg: str):
    """ "Log an error message."""
    print(f"[-] {msg}")


# -----------------------------------------------------------------------------
# Argument Parsing
# -----------------------------------------------------------------------------


def parse_arguments() -> Tuple[str, str]:
    """
    Parse commandline arguments.
    :return (ddl, output_dir)
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("ddl", help="The path to the input DDL file.", type=str)
    parser.add_argument(
        "output_dir", help="The path to the output directory.", type=str
    )
    args = parser.parse_args()
    return args.ddl, args.output_dir


# -----------------------------------------------------------------------------
# Loading Table Metadata
# -----------------------------------------------------------------------------


class Column:
    """
    The column class represents a schema column.
    """

    @staticmethod
    def from_column_definition(column_definition: str):
        """
        Construct a Column from a column definition string.
        :param The column definition string
        :return The Column
        """
        name, type = column_definition.strip().split()[:2]
        return Column(name, type.strip(","))

    def __init__(self, name: str, type: str):
        """
        Initialize a new Column instance.
        :param name The name of the column
        :param type The type of the column
        """
        self.name = name
        self.type = type

    def __str__(self):
        """Return a string representation of the column."""
        return f"({self.name} : {self.type})"


class Schema:
    """
    The Schema class represents a table schema.
    """

    @staticmethod
    def from_column_definitions(column_definitions: List[str]):
        """
        Construct a Schema instance from a list of column definitions.
        :param column_definitions The list of column definitions
        :return The Schema
        """
        return Schema([Column.from_column_definition(d) for d in column_definitions])

    def __init__(self, columns: List[Column]):
        """
        Initialize a new Schema instance.
        :param columns The list of columns
        """
        self.columns = columns

    def __str__(self):
        """Return a string representation of the schema."""
        n_columns = len(self.columns)
        name_width = max(len(c.name) for c in self.columns)
        s = "(\n"
        for idx, column in enumerate(self.columns):
            comma = "," if idx < n_columns - 1 else ""
            s += f"\t{column.name:<{name_width}} {column.type}{comma}\n"
        s += ")"
        return s


class TableMeta:
    """
    The TableMeta class describes a table.
    """

    @staticmethod
    def from_statement(statement: str):
        """
        Factory function to construct a TableMeta
        instance from a CREATE TABLE statement.
        :param statement The statement
        :return The TableMeta instance
        """
        lines = [
            l
            for l in statement.split("\n")
            if l != ")" and l != "(" and len(l.strip()) > 0
        ]
        if not lines[0].startswith("CREATE TABLE"):
            raise RuntimeError("Malformed schema file")

        # Extract the table name
        name = lines[0].strip("CREATE TABLE").strip()
        # Extract the table columns
        schema = Schema.from_column_definitions(lines[1:])
        return TableMeta(name, schema)

    def __init__(self, name: str, schema: Schema):
        """
        Initialize a new TableMeta instance.
        :param name The name of the table
        :param schema The table schema
        """
        # The name of the table
        self.name = name
        # The table schema
        self.schema = schema

    def __str__(self):
        """Return a string representation of the table metdata."""
        return f"{self.name}\n{self.schema}"


def load_table_metadata(schema_file: str) -> List[TableMeta]:
    """
    Load all SQL ProcBench schemas from the DDL file.
    :param schema The path to the schema file
    :return A list of table metadata objects
    """
    with open(schema_file, "r") as f:
        statements = [s for s in map(str.strip, f.read().split(";")) if len(s) > 0]
        return [TableMeta.from_statement(s) for s in statements]


# -----------------------------------------------------------------------------
# Schema File Generation
# -----------------------------------------------------------------------------


def clean_column_type(type: str) -> Tuple[str, str]:
    """
    Construct a string suitable for schema file from column type.
    :param type The column type as a string
    :return The cleaned column type string
    """
    type = type.lower()
    if type == "integer" or type == "float8" or type == "date":
        return type, ""
    if type.startswith("char"):
        # Slice off any width specifier for CHAR
        return type[: len("char")], ""
    if type.startswith("varchar"):
        # Slice off any width specifier for VARCHAR
        m = re.search("((\d+))", type)
        if m is None:
            raise RuntimeError(f"Malformed VARCHAR Type {type}")
        b, e = m.span()
        return f"{type[:len('varchar')]}", f"{type[b : e]}"
    raise RuntimeError(f"Unknown Type in DDL {type}")


def generate_schema(table_meta: TableMeta, output_dir: str):
    """
    Generate the schema file for the tabl described by `table_meta`.
    :param table_meta The table metadata object
    :param output_dir The directory to which schema file is saved
    """
    # The name of the schema file
    path = os.path.join(output_dir, f"{table_meta.name}.schema")
    with open(path, "w") as f:
        # Write the header row
        f.write(f"{table_meta.name} {len(table_meta.schema.columns)}\n")
        # Write the descriptor for each of the columns
        for i, column in enumerate(table_meta.schema.columns):
            c, l = clean_column_type(column.type)
            newline = "" if i + 1 == len(table_meta.schema.columns) else "\n"
            f.write(f"{column.name} {c} 0 {l}{newline}")


def generate_schemas(ddl, output_dir):
    # Load the table metadata for each table defined in the DDL file
    for tm in load_table_metadata(ddl):
        generate_schema(tm, output_dir)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> int:
    ddl, output_dir = parse_arguments()
    try:
        generate_schemas(ddl, output_dir)
    except RuntimeError as e:
        error(f"{e}")
        return EXIT_SUCCESS

    return EXIT_SUCCESS


# -----------------------------------------------------------------------------
# Script Entry
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    sys.exit(main())
