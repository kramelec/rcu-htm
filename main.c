#include <stdio.h>

#include "arch.h"
#include "clargs.h"
#include "benchmarks.h"

void get_clargs(int argc, char **argv)
{
	clargs_init(argc, argv);
	clargs_print();
}

int main(int argc, char **argv)
{
	int ret = 0;
	
	get_clargs(argc, argv);

	ret = bench_pthreads();

	return ret;
}
