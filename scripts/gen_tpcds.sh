#!/bin/bash
# gen_tpcds.sh
# Generate TPC-DS tables. 
#
# Usage:
#   gen_tpcds.sh <SCALE_FACTOR>

# Build dbgen
make -C tpcds-dbgen dsdgen

# Run dbgen
cd tpcds-dbgen && ./dsdgen -force -scale 1
cd ..

# Move the generated tables to the `tables` directory
mv tpcds-dbgen/*.dat tpcds-tables
for f in tpcds-tables/*.dat; do 
    mv -- "$f" "${f%.dat}.tbl"
done

echo "Subsetting for scale factor..."

python scripts/scale.py tpcds-tables/ $1

echo "Done"

echo "Formatting..."

# Process and clean tables
python scripts/clean.py tpcds-tables/

# Construct the custom tables for ProcBench
cp tpcds-tables/catalog_returns.data tpcds-tables/catalog_returns_history.data
cp tpcds-tables/catalog_sales.data tpcds-tables/catalog_sales_history.data
cp tpcds-tables/inventory.data tpcds-tables/inventory_history.data
cp tpcds-tables/store_returns.data tpcds-tables/store_returns_history.data
cp tpcds-tables/store_sales.data tpcds-tables/store_sales_history.data
cp tpcds-tables/web_returns.data tpcds-tables/web_returns_history.data
cp tpcds-tables/web_sales.data tpcds-tables/web_sales_history.data

# Remove the source tables
rm tpcds-tables/*.tbl

echo "Done"
exit 0
