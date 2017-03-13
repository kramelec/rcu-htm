#ifndef __AFF_H__
#define __AFF_H__

void setaffinity_oncpu(unsigned int cpu);
void get_mtconf_options(unsigned int *nr_cpus, unsigned int **cpus);
void mt_conf_print(unsigned int ncpus, unsigned int *cpus);

#endif /* __AFF_H */
