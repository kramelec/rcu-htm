#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <getopt.h>
#include "clargs.h"

/* Default command line arguments */
#define ARGUMENT_DEFAULT_NUM_THREADS 1
#define ARGUMENT_DEFAULT_INIT_TREE_SIZE 100000
#define ARGUMENT_DEFAULT_MAX_KEY (2 * ARGUMENT_DEFAULT_INIT_TREE_SIZE) 
#define ARGUMENT_DEFAULT_LOOKUP_FRAC 0
#define ARGUMENT_DEFAULT_INSERT_FRAC 50
#define ARGUMENT_DEFAULT_INIT_SEED 1024
#define ARGUMENT_DEFAULT_THREAD_SEED 128
#ifdef WORKLOAD_TIME
#define ARGUMENT_DEFAULT_RUN_TIME_SEC 5
#elif defined WORKLOAD_FIXED
#define ARGUMENT_DEFAULT_NR_OPERATIONS 1000000
#endif

static char *opt_string = "ht:s:m:i:l:r:e:j:o:";
static struct option long_options[] = {
	{ "help",            no_argument,       NULL, 'h' },
	{ "num-threads",     required_argument, NULL, 't' },
	{ "init-tree",       required_argument, NULL, 's' },
	{ "max-key",         required_argument, NULL, 'm' },
	{ "lookup-frac",     required_argument, NULL, 'l' },
	{ "insert-frac",     required_argument, NULL, 'i' },
	/* FIXME better short options for these, or no short */
	{ "init-seed",       required_argument, NULL, 'e' },
	{ "thread-seed",     required_argument, NULL, 'j' },

#	if defined(WORKLOAD_FIXED)
	{ "nr-operations",   required_argument, NULL, 'o' },
#	elif defined(WORKLOAD_TIME)
	{ "run-time-sec",    required_argument, NULL, 'r' },
#	endif

	{ NULL, 0, NULL, 0 }
};

clargs_t clargs = {
	ARGUMENT_DEFAULT_NUM_THREADS,
	ARGUMENT_DEFAULT_INIT_TREE_SIZE,
	ARGUMENT_DEFAULT_MAX_KEY,
	ARGUMENT_DEFAULT_LOOKUP_FRAC,
	ARGUMENT_DEFAULT_INSERT_FRAC,
	ARGUMENT_DEFAULT_INIT_SEED,
	ARGUMENT_DEFAULT_THREAD_SEED,
#	ifdef WORKLOAD_TIME
	ARGUMENT_DEFAULT_RUN_TIME_SEC
#	elif defined(WORKLOAD_FIXED)
	ARGUMENT_DEFAULT_NR_OPERATIONS
#	endif
};

static void clargs_print_usage(char *progname)
{
	printf("usage: %s [options]\n"
	       "  possible options:\n"
	       "    -h,--help  print this help message\n"
	       "    -t,--num-threads  number of threads [%d]\n"
	       "    -s,--init-tree  number of elements the initial tree contains [%d]\n"
	       "    -m,--max-key  max key to lookup,insert,delete [%d]\n"
	       "    -l,--lookup-frac  lookup fraction of operations [%d%%]\n"
	       "    -i,--insert-frac  insert fraction of operations [%d%%]\n"
	       "    -e,--init-seed    the seed that is used for the tree initializion [%d]\n"
	       "    -j,--thread-seed  the seed that is used for the thread operations [%d]\n",
	       progname, ARGUMENT_DEFAULT_NUM_THREADS, ARGUMENT_DEFAULT_INIT_TREE_SIZE,
	       ARGUMENT_DEFAULT_MAX_KEY, ARGUMENT_DEFAULT_LOOKUP_FRAC, 
	       ARGUMENT_DEFAULT_INSERT_FRAC,
	       ARGUMENT_DEFAULT_INIT_SEED, ARGUMENT_DEFAULT_THREAD_SEED);

#	ifdef WORKLOAD_TIME
	printf("    -r,--run-time-sec execution time [%d sec]\n",
	        ARGUMENT_DEFAULT_RUN_TIME_SEC);
#	elif defined(WORKLOAD_FIXED)
	printf("    -o,--nr-operations number of operations to execute [%d]\n",
	        ARGUMENT_DEFAULT_NR_OPERATIONS);
#	endif
}

void clargs_init(int argc, char **argv)
{
	char c;
	int i;

	while (1) {
		i = 0;
		c = getopt_long(argc, argv, opt_string, long_options, &i);
		if (c == -1)
			break;

		switch(c) {
		case 'h':
			clargs_print_usage(argv[0]);
			exit(1);
		case 't':
			clargs.num_threads = atoi(optarg);
			break;
		case 's':
			clargs.init_tree_size = atoi(optarg);
			break;
		case 'm':
			clargs.max_key = atoi(optarg);
			break;
		case 'l':
			clargs.lookup_frac = atoi(optarg);
			break;
		case 'i':
			clargs.insert_frac = atoi(optarg);
			break;
		case 'e':
			clargs.init_seed = atoi(optarg);
			break;
		case 'j':
			clargs.thread_seed = atoi(optarg);
			break;
#		ifdef WORKLOAD_TIME
		case 'r':
			clargs.run_time_sec = atoi(optarg);
			break;
#		elif defined(WORKLOAD_FIXED)
		case 'o':
			clargs.nr_operations = atoi(optarg);
			break;
#		endif
		default:
			clargs_print_usage(argv[0]);
			exit(1);
		}
	}

	/* Sanity checks. */
	assert(clargs.lookup_frac + clargs.insert_frac <= 100);
}

void clargs_print()
{
	printf("Inputs:\n"
	       "====================\n"
	       "  num_threads: %d\n"
	       "  init_tree_size: %d\n"
	       "  max_key: %d\n"
	       "  lookup_frac: %d\n"
	       "  insert_frac: %d\n"
	       "  init_seed: %d\n"
	       "  thread_seed: %d\n",
	       clargs.num_threads, clargs.init_tree_size, clargs.max_key,
	       clargs.lookup_frac, clargs.insert_frac,
	       clargs.init_seed, clargs.thread_seed);

#	ifdef WORKLOAD_TIME
	printf("  run_time_sec: %d\n", clargs.run_time_sec);
#	elif defined(WORKLOAD_FIXED)
	printf("  nr_operations: %d\n", clargs.nr_operations);
#	endif

	printf("\n");
}
