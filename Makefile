MODULE_big = pg_check
OBJS = src/pg_check.o src/common.o src/heap.o src/index.o

EXTENSION = pg_check
DATA = sql/pg_check--0.1.0.sql
MODULES = pg_check

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

CFLAGS=`pg_config --includedir-server`

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

pg_check.so: $(OBJS)
