#-------------------------------------------------------------------------
#
# pg_check Makefile
#
#-------------------------------------------------------------------------

MODULE_big	= pg_check
OBJS		= pg_check.o index.o heap.o common.o
DATA_built	= pg_check.sql
DATA      	= uninstall_pg_check.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_check
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

