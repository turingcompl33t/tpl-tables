# Makefile

# The scale factor for table generation
SCALE_FACTOR=1

default:
	@echo No default target!

# Generate TPC-H tables
.PHONY: generate-tpch
generate-tpch:
	./scripts/gen_tpch.sh ${SCALE_FACTOR}

# Generate TPC-DS tables
.PHONY: generate-tpcds
generate-tpcds:
	./scripts/gen_tpcds.sh ${SCALE_FACTOR}

.PHONY: clean
clean:
	# Clean build tools
	make -C tpch-dbgen clean
	make -C tpcds-dbgen clean
	# Clean data files
	rm -f tpch-tables/*.data
	rm -f tpcds-tables/*.data

.PHONY: format
format:
	black scripts/*.py
	black utility/*.py
