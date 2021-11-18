#!/bin/bash
# gen_tpch.sh
# Generate TPC_H tables. 
#
# Usage:
#   gen_tpch.sh <SCALE_FACTOR>

# Build dbgen
make -C tpch-dbgen

# Run dbgen
cd tpch-dbgen && ./dbgen -f -s $1
cd ..

# Move the generated tables to the `tables` directory
mv tpch-dbgen/*.tbl tpch-tables

echo "Formatting..."

# Process and clean raw tables
python scripts/clean.py tpch-tables/

# Remove the source files
rm tpch-tables/*.tbl

echo "Formatted"

exit 0
