#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#ifdef WORKLOAD_TIME
#	include <unistd.h> //> sleep()
#endif

#include "alloc.h"
#include "aff.h"
#include "clargs.h"
#include "rbt/iface.h"
#include "timers_lib.h"
#include "arch.h"

// DEBUG
static inline unsigned long
xorshf96(unsigned long* x, unsigned long* y, unsigned long* z)
{
  unsigned long t;
  (*x) ^= (*x) << 16;
  (*x) ^= (*x) >> 5;
  (*x) ^= (*x) << 1;

  t = *x;
  (*x) = *y;
  (*y) = *z;
  (*z) = t ^ (*x) ^ (*y);

  return *z;
}

//> Enumeration for indexing the operations_{performed,succeeded} arrays.
enum {
	OPS_TOTAL = 0,
	OPS_LOOKUP,
	OPS_INSERT,
	OPS_DELETE,
	OPS_END
};

typedef struct {
	int tid;
	int cpu;
	
	void *rbt;

#	if defined(WORKLOAD_FIXED)
	int nr_operations;
#	elif defined(WORKLOAD_TIME)
	int *time_to_leave;
#	endif

	int operations_performed[4],
	    operations_succeeded[4];

	void *rbt_thread_data;

	char padding[CACHE_LINE_SIZE - 11 * sizeof(int) - sizeof(void *)];
} __attribute__((aligned(CACHE_LINE_SIZE))) thread_data_t;

static inline thread_data_t *thread_data_new(int tid, int cpu, void *rbt)
{
	thread_data_t *ret;

	XMALLOC(ret, 1);
	memset(ret, 0, sizeof(*ret));
	ret->tid = tid;
	ret->cpu = cpu;
	ret->rbt = rbt;

	return ret;
}

static inline void thread_data_print(thread_data_t *data)
{
	int i;
	printf("%3d %3d", data->tid, data->cpu);
	for (i=0; i < 4; i++)
		printf(" %10d %10d", data->operations_performed[i], 
		                     data->operations_succeeded[i]);
	printf("\n");
}

static inline void thread_data_print_rbt_data(thread_data_t *data)
{
	rbt_thread_data_print(data->rbt_thread_data);
}

static inline void thread_data_add_rbt_data(thread_data_t *d1, thread_data_t *d2,
                              thread_data_t *dst)
{
	rbt_thread_data_add(d1->rbt_thread_data, d2->rbt_thread_data, 
	                    dst->rbt_thread_data);
}

static inline void thread_data_add(thread_data_t *d1, thread_data_t *d2, 
                                   thread_data_t *dest)
{
	int i = 0;

#	if defined(WORKLOAD_FIXED)
	dest->nr_operations = d1->nr_operations + d2->nr_operations;
#	endif

	for (i=0; i < OPS_END; i++) {
		dest->operations_performed[i] = d1->operations_performed[i] + 
		                                d2->operations_performed[i];
		dest->operations_succeeded[i] = d1->operations_succeeded[i] + 
		                                d2->operations_succeeded[i];
	}
}

pthread_barrier_t sync_barrier;
pthread_barrier_t start_barrier;

void *thread_fn(void *arg)
{
	int ops_performed = 0, ret;
	thread_data_t *data = arg;
	int tid = data->tid, cpu = data->cpu;
	void *rbt = data->rbt;
	int choice, key_int;
	
	//> For thread_safe (and scalable) random number generation.
	struct drand48_data drand_buffer;
	long int drand_res;

	srand48_r((data->tid + 1) * clargs.thread_seed, &drand_buffer);

	//> Set affinity.
	setaffinity_oncpu(cpu);

	//> Initialize per thread red-black tree data.
	data->rbt_thread_data = rbt_thread_data_new(tid);

	//> Wait for the master to give the starting signal.
	pthread_barrier_wait(&start_barrier);

	int i = 0;

//	unsigned long seeds[3] = {tid*100 + 1, tid*100 + 2, tid*100 + 3};

	//> Critical section.
	while (1) {
#		if defined(WORKLOAD_FIXED)
		if (ops_performed >= data->nr_operations - 1)
			break;
#		elif defined(WORKLOAD_TIME)
		if (*(data->time_to_leave))
			break;
#		endif

		ops_performed = data->operations_performed[OPS_TOTAL]++;

		//> Generate random number;
		lrand48_r(&drand_buffer, &drand_res);
		choice = drand_res % 100;
		lrand48_r(&drand_buffer, &drand_res);
		key_int = drand_res % clargs.max_key;
//		unsigned long rand = xorshf96(&seeds[0], &seeds[1], &seeds[2]);
//		choice = rand % 100;
//		key_int = rand % clargs.max_key;
		int key = key_int;
//		key = key % 1000;
//		char *key = keys[i%NR_KEYS];
////		char *key;
////		XMALLOC(key, 256);
////		for (i=0; i < 900; i++)
////			key[i] = 'a';
//		sprintf(key, "%030d", key_int);
//		i++;

//		if (tid < clargs.num_threads / 2) {
//		if (tid == 0) {
//			choice = 40;
//		} else {
//			choice = choice % 50 + 50;
//		}

//		if (choice < 50) choice += 50;
//		if (tid < clargs.num_threads / 2)
//			choice = 30;

////		choice = 30;
//		if (i % 2) choice = 30; else choice = 55;
//		i++;
//		pthread_barrier_wait(&sync_barrier);
//		if (tid == 0) {
//			printf("----------------------------------------------------\n");
//			rbt_print(rbt);
//		}
//		pthread_barrier_wait(&sync_barrier);
//		printf("TID: %d KEY: %d CHOICE: %d\n", tid, key, choice);
//		int lol = rbt_lookup(rbt, data->rbt_thread_data, key);
//		printf("TID: %d KEY: %d CHOICE: %d (IN_TREE = %d)\n", tid, key, choice, lol);
//		pthread_barrier_wait(&sync_barrier);
//		if (tid == 0 && rbt_validate(rbt) != 1)
//			exit(1);
//		pthread_barrier_wait(&sync_barrier);

		//> Perform operation on the RBT based on choice.
		if (choice < clargs.lookup_frac) {
			//> Lookup
			data->operations_performed[OPS_LOOKUP]++;
			ret = rbt_lookup(rbt, data->rbt_thread_data, key);
			data->operations_succeeded[OPS_LOOKUP] += ret;
		} else if (choice  < clargs.lookup_frac + clargs.insert_frac) {
			//> Insertion
			data->operations_performed[OPS_INSERT]++;
			ret = rbt_insert(rbt, data->rbt_thread_data, key, NULL);
			data->operations_succeeded[OPS_INSERT] += ret;

//			if (ret == 1 && rbt_lookup(rbt, data->rbt_thread_data, key) == 0) {
//				printf("SKAAAAAATAAAAAA INSERT\n");
//				exit(128);
//			}
		} else {
			//> Deletion
			data->operations_performed[OPS_DELETE]++;
			ret = rbt_delete(rbt, data->rbt_thread_data, key);
			data->operations_succeeded[OPS_DELETE] += ret;

//			if (ret == 1 && rbt_lookup(rbt, data->rbt_thread_data, key) != 0) {
//				printf("SKAAAAAATAAAAAA DELETE\n");
//				exit(128);
//			}
		}
		data->operations_succeeded[OPS_TOTAL] += ret;

//		if (ret == 1 && choice >= clargs.lookup_frac + clargs.insert_frac) {
//			printf("KEY = %d\n", key);
//			rbt_validate(rbt);
//
//			printf("Expected size of RBT: %d\n", clargs.init_tree_size +
//			        data->operations_succeeded[OPS_INSERT] - 
//			        data->operations_succeeded[OPS_DELETE]);
//		}

//		pthread_barrier_wait(&sync_barrier);
	}

	return NULL;
}

int bench_pthreads()
{
	int i, validation;
	int nthreads = clargs.num_threads;
	pthread_t *threads;
	thread_data_t **threads_data;
	void *rbt;
	int time_to_leave = 0;
	timer_tt *warmup_timer;

	//> Initialize Red-Black tree.
	rbt = rbt_new();
	printf("\nBenchmark\n");
	printf("=======================\n");
	printf("  Name: Parallel(pthreads)\n");
	printf("  RBT implementation: %s\n", rbt_name());

	//> Red-Black tree warmup.
	warmup_timer = timer_init();
	printf("\n");
	printf("Tree initialization... ");
	fflush(stdout);
	timer_start(warmup_timer);
	rbt_warmup(rbt, clargs.init_tree_size, clargs.max_key, clargs.init_seed, 0);
	timer_stop(warmup_timer);
	printf("[OK (%5.2lf sec)]\n", timer_report_sec(warmup_timer));

	//> Initialize the starting barrier.
	pthread_barrier_init(&start_barrier, NULL, nthreads+1);
	pthread_barrier_init(&sync_barrier, NULL, nthreads);
	
	//> Initialize the arrays that hold the thread references and data.
	XMALLOC(threads, nthreads);
	XMALLOC(threads_data, nthreads);

	//> Initialize per thread data and spawn threads.
	for (i=0; i < nthreads; i++) {
		int cpu = i;
//		cpu %= 22;
		if (cpu >= 22)
			cpu += 22;
//		cpu = (28 * (i%2)) + (i/2); // HT first
//		cpu = (14 * (i%2)) + (i/2); // Numa first
//		if (i >= 28)
//			cpu += 14;
//		if (i >= 14 && i <= 27)
//			cpu += 14;
#		if defined(__POWERPC64__)
		cpu = i * 8 % 160 + i * 8 / 160;
#		endif
		threads_data[i] = thread_data_new(i, cpu, rbt);
#		ifdef WORKLOAD_FIXED
		threads_data[i]->nr_operations = clargs.nr_operations / nthreads;
#		elif defined(WORKLOAD_TIME)
		threads_data[i]->time_to_leave = &time_to_leave;
#		endif
		pthread_create(&threads[i], NULL, thread_fn, threads_data[i]);
	}

	//> Wait until all threads go to the starting point.
	pthread_barrier_wait(&start_barrier);

	//> Init and start wall_timer.
	timer_tt *wall_timer = timer_init();
	timer_start(wall_timer);

#	if defined(WORKLOAD_TIME)
	sleep(clargs.run_time_sec);
	time_to_leave = 1;
#	endif

	//> Join threads.
	for (i=0; i < nthreads; i++)
		pthread_join(threads[i], NULL);

	//> Stop wall_timer.
	timer_stop(wall_timer);

	//> Print thread statistics.
	thread_data_t *total_data = thread_data_new(-1, -1, NULL);
	printf("\nThread statistics\n");
	printf("=======================\n");
	for (i=0; i < nthreads; i++) {
		thread_data_print(threads_data[i]);
		thread_data_add(threads_data[i], total_data, total_data);
	}
	printf("-----------------------\n");
	thread_data_print(total_data);

	//> Print additional per thread statistics.
	total_data->rbt_thread_data = rbt_thread_data_new(-1);
	printf("\n");
	printf("\nAdditional per thread statistics\n");
	printf("=======================\n");
	for (i=0; i < nthreads; i++) {
		thread_data_print_rbt_data(threads_data[i]);
		thread_data_add_rbt_data(threads_data[i], total_data, total_data);
	}
	printf("-----------------------\n");
	thread_data_print_rbt_data(total_data);
	printf("\n");

	//> Validate the final RBT.
	validation = rbt_validate(rbt);

	//> Print elapsed time.
	double time_elapsed = timer_report_sec(wall_timer);
	double throughput_usec = total_data->operations_performed[OPS_TOTAL] / 
	                         time_elapsed / 1000000.0;
	printf("Time elapsed: %6.2lf\n", time_elapsed);
	printf("Throughput(Ops/usec): %7.3lf\n", throughput_usec);

	printf("Expected size of RBT: %d\n", clargs.init_tree_size +
	        total_data->operations_succeeded[OPS_INSERT] - 
	        total_data->operations_succeeded[OPS_DELETE]);

	return validation;
}
