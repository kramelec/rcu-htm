CC = gcc
CFLAGS = -Wall -Wextra -g -O3

## Ignore unused variable/parameter warnings.
CFLAGS += -Wno-unused-variable -Wno-unused-parameter
CFLAGS += -Wno-unused-but-set-variable

## For which architecture are we building?
ARCH = $(shell uname -m)
ifeq ($(ARCH), ppc64le)
	## char in powerpc defaults to unsigned char
	ARCH_FLAGS += -fsigned-char
	ARCH_FLAGS += -mhtm 
endif
CFLAGS += $(ARCH_FLAGS)

## Number of transactional retries before resorting to non-tx fallback.
CFLAGS += -DTX_NUM_RETRIES=10

## Which workload do we want?
WORKLOAD_FLAG = -DWORKLOAD_TIME
#WORKLOAD_FLAG = -DWORKLOAD_FIXED
CFLAGS += $(WORKLOAD_FLAG)

INC_FLAGS = -Ilib/
CFLAGS += $(INC_FLAGS)
CFLAGS += -pthread

SOURCE_FILES = main.c lib/clargs.c lib/aff.c bench_pthreads.c

all: x.rbt.int.rcu_htm x.avl.int.rcu_htm

x.rbt.int.rcu_htm: $(SOURCE_FILES) rbt-rcu-htm-internal.c
	$(CC) $(CFLAGS) $^ -o $@
x.avl.int.rcu_htm: $(SOURCE_FILES) avl-rcu-htm-internal.c
	$(CC) $(CFLAGS) $^ -o $@

clean:
	rm -f x.*
