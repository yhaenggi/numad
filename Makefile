# these can (and should) be overridden on the make command line for production
CFLAGS := -std=gnu99
# these are used for the benchmarks in addition to the normal CFLAGS. 
# Normally no need to overwrite unless you find a new magic flag to make
# STREAM run faster.
BENCH_CFLAGS := -O3 -ffast-math -funroll-loops
# for compatibility with old releases
CFLAGS += ${OPT_CFLAGS}
override CFLAGS += -I.

# find out if compiler supports __thread
THREAD_SUPPORT := $(shell if $(CC) $(CFLAGS) threadtest.c -o threadtest \
			>/dev/null 2>/dev/null ; then echo "yes" ; else echo "no"; fi)
ifeq ($(THREAD_SUPPORT),no)
	override CFLAGS += -D__thread=""
endif

# find out if compiler supports -ftree-vectorize
THREAD_SUPPORT := $(shell touch empty.c ; if $(CC) $(CFLAGS) -c -ftree-vectorize empty.c -o empty.o \
			>/dev/null 2>/dev/null ; then echo "yes" ; else echo "no"; fi)
ifeq ($(THREAD_SUPPORT),yes)
	BENCH_CFLAGS += -ftree-vectorize
endif

CLEANFILES := numad.o numad .depend .depend.X empty.c empty.o

SOURCES := numad.c

prefix := /usr
docdir := ${prefix}/share/doc

all: numad

numad: numad.o -lpthread -lrt

AR ?= ar
RANLIB ?= ranlib

.PHONY: install all clean html depend

# BB_FIXME MANPAGES := numa.3 numactl.8 numastat.8 migratepages.8 migspeed.8

install: numad
	mkdir -p ${prefix}/bin
	mkdir -p ${prefix}/share/man/man8
	install -m 0755 numad ${prefix}/bin
	install -m 0644 numad.8 ${prefix}/share/man/man8

clean: 
	rm -f ${CLEANFILES}
	@rm -rf html

distclean: clean
	rm -f .[^.]* */.[^.]*
	rm -f *~ */*~ *.orig */*.orig */*.rej *.rej 

depend: .depend

.depend:
	${CC} -MM -DDEPS_RUN -I. ${SOURCES} > .depend.X && mv .depend.X .depend

include .depend

Makefile: .depend

