#ifndef _HTM_POWER_
#define _HTM_POWER_

#include <stdio.h>
#include <string.h>
#include <pthread.h> /* pthread_spinlock_t */
#include <htmintrin.h>

#include "alloc.h" /* XMALLOC */

enum {
	TX_ABORT_TRANSACTIONAL,
	TX_ABORT_NON_TRANSACTIONAL,
	TX_ABORT_FOOTPRINT_OVERFLOW,
	TX_ABORT_EXPLICIT,
	TX_ABORT_DISALLOWED,
	TX_ABORT_NESTING_OVERFLOW,
	TX_ABORT_SELF_INDUCED,
	TX_ABORT_TRANSLATION_INVALIDATION,
	TX_ABORT_IMPLEMENTATION_SPECIFIC,
	TX_ABORT_INSTRUCTION_FETCH,
	TX_ABORT_REST,
	TX_ABORT_REASONS_END
};

//> Some per thread statistics.
typedef struct {
	int tid;
	int tx_starts,
	    tx_commits,
	    tx_aborts,
	    tx_lacqs;

	int tx_aborts_per_reason[TX_ABORT_REASONS_END];

} tx_thread_data_t;

#ifdef USE_CPU_LOCK
#include <semaphore.h> /* sem_t */
#include "arch.h"
#define NR_CPUS 20
static struct {
	pthread_spinlock_t spinlock;
	int owner; /* A per cpu lock. Its value is the pid of the owner. */
	char padding[CACHE_LINE_SIZE - sizeof(int) - sizeof(pthread_spinlock_t)];
} __attribute__((aligned(CACHE_LINE_SIZE))) cpu_locks[NR_CPUS];
#endif

static inline void *tx_thread_data_new(int tid)
{
	tx_thread_data_t *ret;

#	ifdef USE_CPU_LOCK
	if (tid == 0) {
		int i;
		for (i=0; i < NR_CPUS; i++) {
			pthread_spin_init(&cpu_locks[i].spinlock, PTHREAD_PROCESS_SHARED);
			cpu_locks[i].owner= -1;
		}
	}
#	endif

	XMALLOC(ret, 1);
	memset(ret, 0, sizeof(*ret));
	ret->tid = tid;
	return ret;
}

static inline void tx_thread_data_print(void *thread_data)
{
	int i;

	if (!thread_data)
		return;

	tx_thread_data_t *data = thread_data;

	printf("TXSTATS(POWER): %3d %12d %12d %12d (", data->tid,
	       data->tx_starts, data->tx_commits, data->tx_aborts);

	for (i=0; i < TX_ABORT_REASONS_END; i++)
		printf(" %12d", data->tx_aborts_per_reason[i]);
	printf(" )");
	printf(" %12d\n", data->tx_lacqs);
}

static inline void tx_thread_data_add(void *d1, void *d2, void *dst)
{
	int i;
	tx_thread_data_t *data1 = d1, *data2 = d2, *dest = dst;

	dest->tx_starts = data1->tx_starts + data2->tx_starts;
	dest->tx_commits = data1->tx_commits + data2->tx_commits;
	dest->tx_aborts = data1->tx_aborts + data2->tx_aborts;
	dest->tx_lacqs = data1->tx_lacqs + data2->tx_lacqs;

	for (i=0; i < TX_ABORT_REASONS_END; i++)
		dest->tx_aborts_per_reason[i] = data1->tx_aborts_per_reason[i] +
		                                data2->tx_aborts_per_reason[i];
}

static inline int tx_start(int num_retries, void *thread_data,
                            pthread_spinlock_t *fallback_lock)
{
	int aborts = num_retries;
	tx_thread_data_t *tdata = thread_data;

	int tid = tdata->tid;
	int my_cpu_lock = (tid * 8 % 160 + tid * 8 / 160) / 8;

	while (1) {
		/* Avoid lemming effect. */
#		ifdef USE_CPU_LOCK
		while (cpu_locks[my_cpu_lock].owner > 0 &&
		       cpu_locks[my_cpu_lock].owner != tid &&
		       cpu_locks[my_cpu_lock].spinlock == 1)
			;
#		endif

		while (*fallback_lock == 1)
			;

		tdata->tx_starts++;

		if (__builtin_tbegin(0)) {
#			ifdef USE_CPU_LOCK
			if (cpu_locks[my_cpu_lock].owner > 0 &&
			    cpu_locks[my_cpu_lock].owner != tid &&
			    cpu_locks[my_cpu_lock].spinlock == 1)
				__builtin_tabort(0x77);
#			endif

			if (*fallback_lock == 1)
				__builtin_tabort(0xff);
			return num_retries - aborts;
		}

		/* Abort comes here. */
		tdata->tx_aborts++;

		texasru_t texasru = __builtin_get_texasru();
		if (_TEXASRU_TRANSACTION_CONFLICT(texasru))
			tdata->tx_aborts_per_reason[TX_ABORT_TRANSACTIONAL]++;
		else if (_TEXASRU_NON_TRANSACTIONAL_CONFLICT(texasru))
			tdata->tx_aborts_per_reason[TX_ABORT_NON_TRANSACTIONAL]++;
		else if (_TEXASRU_FOOTPRINT_OVERFLOW(texasru))
			tdata->tx_aborts_per_reason[TX_ABORT_FOOTPRINT_OVERFLOW]++;
		else if (_TEXASRU_ABORT(texasru))
			tdata->tx_aborts_per_reason[TX_ABORT_EXPLICIT]++;
		else if (_TEXASRU_DISALLOWED(texasru))
			tdata->tx_aborts_per_reason[TX_ABORT_DISALLOWED]++;
		else if (_TEXASRU_NESTING_OVERFLOW(texasru))
			tdata->tx_aborts_per_reason[TX_ABORT_NESTING_OVERFLOW]++;
		else if (_TEXASRU_SELF_INDUCED_CONFLICT(texasru))
			tdata->tx_aborts_per_reason[TX_ABORT_SELF_INDUCED]++;
		else if (_TEXASRU_TRANSLATION_INVALIDATION_CONFLICT(texasru))
			tdata->tx_aborts_per_reason[TX_ABORT_TRANSLATION_INVALIDATION]++;
		else if (_TEXASRU_IMPLEMENTAION_SPECIFIC(texasru))
			tdata->tx_aborts_per_reason[TX_ABORT_IMPLEMENTATION_SPECIFIC]++;
		else if (_TEXASRU_INSTRUCTION_FETCH_CONFLICT(texasru))
			tdata->tx_aborts_per_reason[TX_ABORT_INSTRUCTION_FETCH]++;
		else
			tdata->tx_aborts_per_reason[TX_ABORT_REST]++;

		if (--aborts <= 0) {
			pthread_spin_lock(fallback_lock);
			return num_retries - aborts;
		}

#		ifdef USE_CPU_LOCK
		if (aborts <= TX_NUM_RETRIES / 2 &&
		    cpu_locks[my_cpu_lock].owner != tid) {
			pthread_spin_lock(&cpu_locks[my_cpu_lock].spinlock);
			cpu_locks[my_cpu_lock].owner = tid;
		}
#		endif
	}

	/* Unreachable. */
	return -1;
}

static inline int tx_end(void *thread_data, pthread_spinlock_t *fallback_lock)
{
	tx_thread_data_t *tdata = thread_data;
	int ret = 0;

	if (*fallback_lock == 1) {
		pthread_spin_unlock(fallback_lock);
		tdata->tx_lacqs++;
		ret = 1;
	} else {
		__builtin_tend (0);
		tdata->tx_commits++;
		ret = 0;
	}

#	ifdef USE_CPU_LOCK
	int tid = tdata->tid;
	int my_cpu_lock = (tid * 8 % 160 + tid * 8 / 160) / 8;
	if (cpu_locks[my_cpu_lock].owner == tid) {
		cpu_locks[my_cpu_lock].owner = -1;
		pthread_spin_unlock(&cpu_locks[my_cpu_lock].spinlock);
	}
#	endif

	return ret;
}

#endif /* _HTM_POWER_ */
