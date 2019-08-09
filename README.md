# Generating Tables for TPL Tests

## General Format
There are two kinds of files, **.data** and **.schema** files:
* **.data** files are simple CSV files containing the data.
* **.schema** files are more complex. They have two parts:
	* The first part specifies the table's schema
	* The second part specifies the indexes on the table, along with the columns they index.

Here is the general structure of a **.schema** file.
```txt
table_name num_cols
col_name1 type nullable <max_varlen_size>
col_name2 type nullable <max_varlen_size>
... (num_cols times)

num_indexes
index_name1 num_index_cols1
table_col1 table_col2 ... (num_index_cols1 times)

index_name2 num_index_cols2
table_col1 table_col2 ... (num_index_cols2 times)
```

## Concrete Example

***types.data***
```txt
int_col,real_col,date_col,varchar_col
0,3.7,2000-01-13,First string
1,8.5,2013-07-17,Second string
2,13.7,2015-09-16,Third string
3,37.37,2037-03-07,Fourth string
```

**types.schema**
```txt
types 4
int_col int 0
real_col decimal 0
date_col date 0
varchar_col varchar 0 20

1
types_index 1
0
```

This creates:
* A table with 4 columns: one integer, one decimal, one date and one varchar(20).
* An index on the table, where the key is the table's first column.

## TPCH Tables
To generate tpch tables with the **dbgen** program, execute:
```sh
bash tpl_test.sh
```

## Usage in TPL tests
To load a table, use:
```C++
// Load a single table
TableGenerator::GenerateTable(schema_file, data_file);
// Load tpch tables
TableGenerator::GenerateTPCHTables(dir_name);
// TODO: Implement this call to load all tables from a directory (requires std::filesystem or boost::filesystem)
TableGenerator::GenerateTablesFromDir(dir_name)
```
