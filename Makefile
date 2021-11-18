# Makefile

SCALE_FACTOR=0.1

default:
	@echo No default target!

.PHONY: generate
generate:
	./scripts/gen_tpch.sh ${SCALE_FACTOR}

.PHONY: clean
clean:
	cd tpch-dbgen && make clean
	rm -f tables/*.data

.PHONY: format
format:
	black scripts/*.py
