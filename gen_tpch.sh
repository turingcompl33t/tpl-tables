# Create tables
cd tpch-dbgen
make
./dbgen -f -s $1
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

# Replace | by ,
#sed -i 's/[|]/,/g' part.tbl
#sed -i 's/[|]/,/g' supplier.tbl
#sed -i 's/[|]/,/g' partsupp.tbl
#sed -i 's/[|]/,/g' customer.tbl
#sed -i 's/[|]/,/g' orders.tbl
#sed -i 's/[|]/,/g' lineitem.tbl
#sed -i 's/[|]/,/g' nation.tbl
#sed -i 's/[|]/,/g' region.tbl

# Rename to .data
mv part.tbl part.data
mv supplier.tbl supplier.data
mv customer.tbl customer.data
mv partsupp.tbl partsupp.data
mv orders.tbl orders.data
mv lineitem.tbl lineitem.data
mv nation.tbl nation.data
mv region.tbl region.data

echo "Formatted"
