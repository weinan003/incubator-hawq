MODULE_big = tpch1
OBJS    = rm.o tpch1.o

PG_CXXFLAGS = -Wall -g -std=c++11
PG_CPPFLAGS = -I$(libpq_srcdir) -I/Users/chwang/Code/incubator-hawq/src/include/resourcemanager
PG_LIBS = $(libpq_pgport) 

PG_CONFIG = pg_config

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/hawq-handcode
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

SHLIB_LINK += $(filter -lz -lsnappy, $(LIBS))
