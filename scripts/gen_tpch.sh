#!/bin/bash
# gen_tpch.sh
# Generate TPCH tables. 
#
# Usage:
#   gen_tpch.sh <SCALE_FACTOR>

# Build dbgen
cd tpch-dbgen
make

# Run dbgen
./dbgen -f -s $1

# Move the generated tables to the `tables` directory
mv *.tbl ../tables
cd ../tables

echo "Formatting..."

# Remove last | in each line
sed -i 's/[|]$//' part.tbl
sed -i 's/[|]$//' supplier.tbl
sed -i 's/[|]$//' partsupp.tbl
sed -i 's/[|]$//' customer.tbl
sed -i 's/[|]$//' orders.tbl
sed -i 's/[|]$//' lineitem.tbl
sed -i 's/[|]$//' nation.tbl
sed -i 's/[|]$//' region.tbl

# Cleanup
python ../scripts/clean_tpch.py part
python ../scripts/clean_tpch.py supplier
python ../scripts/clean_tpch.py partsupp
python ../scripts/clean_tpch.py customer
python ../scripts/clean_tpch.py orders
python ../scripts/clean_tpch.py lineitem
python ../scripts/clean_tpch.py nation
python ../scripts/clean_tpch.py region

rm *.tbl

# Rename to .data
#mv part.tbl part.data
#mv supplier.tbl supplier.data
#mv customer.tbl customer.data
#mv partsupp.tbl partsupp.data
#mv orders.tbl orders.data
#mv lineitem.tbl lineitem.data
#mv nation.tbl nation.data
#mv region.tbl region.data

echo "Formatted"
exit 0
