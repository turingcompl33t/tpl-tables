#!/bin/bash
# gen_tpcds.sh
# Generate TPC-DS tables. 
#
# Usage:
#   gen_tpcds.sh <SCALE_FACTOR>

# Build dbgen
make -C tpcds-dbgen dsdgen

# Run dbgen
cd tpcds-dbgen && ./dsdgen -force -scale $1
cd ..

# Move the generated tables to the `tables` directory
mv tpcds-dbgen/*.dat tpcds-tables
for f in tpcds-tables/*.dat; do 
    mv -- "$f" "${f%.dat}.tbl"
done

echo "Formatting..."

# Process and clean tables
python scripts/clean.py tpcds-tables/

# Remove the source tables
rm tpcds-tables/*.tbl

echo "Formatted"
exit 0
