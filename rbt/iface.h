#ifndef _RBT_IFACE_H_
#define _RBT_IFACE_H_

//> Not thread-safe interface functions.
//> Should only be called during initialization and termination phase
//> by one thread.
void *rbt_new();
char *rbt_name();
int rbt_warmup(void *rbt, int nr_nodes, int max_key, 
               unsigned int seed, int force);
int rbt_validate(void *rbt);

//> Initialize per thread statistics.
void *rbt_thread_data_new(int tid);
void rbt_thread_data_print(void *thread_data);
void rbt_thread_data_add(void *d1, void *d2, void *dst);

//> Thread-safe interface functions.
//> Can handle multiple threads at the same time and produce correct results.
//> XXX: the 'serial' versions are not thread-safe and are provided only for
//>      testing that an error is produced while called by multiple threads.
int rbt_lookup(void *rbt, void *thread_data, int key);
int rbt_insert(void *rbt, void *thread_data, int key, void *value);
int rbt_delete(void *rbt, void *thread_data, int key);
//int rbt_lookup(void *rbt, void *thread_data, char *key);
//int rbt_insert(void *rbt, void *thread_data, char *key, void *value);
//int rbt_delete(void *rbt, void *thread_data, char *key);

int rbt_print(void *rbt);

#endif /* _RBT_IFACE_H_ */
