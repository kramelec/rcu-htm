#include <assert.h>
#include <pthread.h> //> pthread_spinlock_t
#include <string.h>  //> memset() for per thread allocator

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

/******************************************************************************/
/* A simple hash table implementation.                                        */
/******************************************************************************/
#define HT_LEN 16
#define HT_MAX_BUCKET_LEN 64
#define HT_GET_BUCKET(key) ((((long long)(key)) >> 4) % HT_LEN)
typedef struct {
	unsigned short bucket_next_index[HT_LEN];
	// Even numbers (0,2,4) are keys, odd numbers are values.
	void *entries[HT_LEN][HT_MAX_BUCKET_LEN * 2];
} ht_t;

ht_t *ht_new()
{
	int i;
	ht_t *ret;

	XMALLOC(ret, 1);
	memset(&ret->bucket_next_index[0], 0, sizeof(ret->bucket_next_index));
	memset(&ret->entries[0][0], 0, sizeof(ret->entries));
	return ret;
}

void ht_reset(ht_t *ht)
{
	memset(&ht->bucket_next_index[0], 0, sizeof(ht->bucket_next_index));
}

void ht_insert(ht_t *ht, void *key, void *value)
{
	int bucket = HT_GET_BUCKET(key);
	unsigned short bucket_index = ht->bucket_next_index[bucket];

	ht->bucket_next_index[bucket] += 2;

	assert(bucket_index < HT_MAX_BUCKET_LEN * 2);

	ht->entries[bucket][bucket_index] = key;
	ht->entries[bucket][bucket_index+1] = value;
}

void *ht_get(ht_t *ht, void *key, int *found)
{
	int bucket = HT_GET_BUCKET(key);
	int i;

	*found = 1;
	for (i=0; i < ht->bucket_next_index[bucket]; i+=2)
		if (key == ht->entries[bucket][i])
			return ht->entries[bucket][i+1];

	*found = 0;
	return NULL;
}

void ht_print(ht_t *ht)
{
	int i, j;

	for (i=0; i < HT_LEN; i++) {
		printf("BUCKET[%3d]:", i);
		for (j=0; j < ht->bucket_next_index[i]; j+=2)
			printf(" (%p, %p)", ht->entries[i][j], ht->entries[i][j+1]);
		printf("\n");
	}
}
/******************************************************************************/

typedef struct {
	int tid;
	long long unsigned tx_starts, tx_aborts, 
	                   tx_aborts_explicit_validation, lacqs;
	unsigned int next_node_to_allocate;
	ht_t *ht;
} tdata_t;

static inline tdata_t *tdata_new(int tid)
{
	tdata_t *ret;
	XMALLOC(ret, 1);
	ret->tid = tid;
	ret->tx_starts = 0;
	ret->tx_aborts = 0;
	ret->tx_aborts_explicit_validation = 0;
	ret->lacqs = 0;
	ret->next_node_to_allocate = 0;
	ret->ht = ht_new();
	return ret;
}

static inline void tdata_print(tdata_t *tdata)
{
	printf("TID %3d: %llu %llu %llu ( %llu )\n", tdata->tid, tdata->tx_starts,
	      tdata->tx_aborts, tdata->tx_aborts_explicit_validation, tdata->lacqs);
}

static inline void tdata_add(tdata_t *d1, tdata_t *d2, tdata_t *dst)
{
	dst->tx_starts = d1->tx_starts + d2->tx_starts;
	dst->tx_aborts = d1->tx_aborts + d2->tx_aborts;
	dst->tx_aborts_explicit_validation = d1->tx_aborts_explicit_validation +
	                                     d2->tx_aborts_explicit_validation;
	dst->lacqs = d1->lacqs + d2->lacqs;
}

/* TM Interface. */
#if !defined(TX_NUM_RETRIES)
#	define TX_NUM_RETRIES 20
#endif

#ifdef __POWERPC64__
#	include <htmintrin.h>
	typedef int tm_begin_ret_t;
#	define LOCK_FREE 0
#	define TM_BEGIN_SUCCESS 1
#	define ABORT_VALIDATION_FAILURE 0xee
#	define ABORT_GL_TAKEN           0xff
#	define TX_ABORT(code) __builtin_tabort(code)
#	define TX_BEGIN(code) __builtin_tbegin(0)
#	define TX_END(code)   __builtin_tend(0)
#else
#	include "rtm.h"
	typedef unsigned tm_begin_ret_t;
#	define LOCK_FREE 1
#	define TM_BEGIN_SUCCESS _XBEGIN_STARTED
#	define ABORT_VALIDATION_FAILURE 0xee
#	define ABORT_GL_TAKEN           0xff
#	define ABORT_IS_CONFLICT(status) ((status) & _XABORT_CONFLICT)
#	define ABORT_IS_EXPLICIT(status) ((status) & _XABORT_EXPLICIT)
#	define ABORT_CODE(status) _XABORT_CODE(status)
#	define TX_ABORT(code) _xabort(code)
#	define TX_BEGIN(code) _xbegin()
#	define TX_END(code)   _xend()
#endif
/*****************/

#define MAX_HEIGHT 50

typedef enum {
	RED = 0,
	BLACK
} color_t;

typedef struct rbt_node {
	color_t color;
	int key;
	void *data;
	struct rbt_node *left, *right;
} __attribute__((packed)) rbt_node_t;

typedef struct {
	rbt_node_t *root;

	// Add extra padding here to avoid having `root` in the
	// same cache line with the lock.
	char padding[CACHE_LINE_SIZE - sizeof(rbt_node_t *)];

	pthread_spinlock_t rbt_lock;
} rbt_t;

unsigned int next_node_to_allocate;
rbt_node_t *per_thread_node_allocators[88];

#define IS_BLACK(node) ( !(node) || (node)->color == BLACK )
#define IS_RED(node) ( !IS_BLACK(node) )

static rbt_node_t *rbt_node_new(int key, color_t color, void *data)
{
	rbt_node_t *node;
	
	XMALLOC(node, 1);
	node->color = color;
	node->key = key;
	node->data = data;
	node->left = NULL;
	node->right = NULL;

	return node;
}

static void rbt_node_copy(rbt_node_t *dest, rbt_node_t *src, tdata_t *tdata)
{
	dest->color = src->color;
	dest->key = src->key;
	dest->left = src->left;
	dest->right = src->right;
	__sync_synchronize();
}

static rbt_node_t *rbt_node_new_copy(rbt_node_t *src, tdata_t *tdata)
{
	rbt_node_t *node = &per_thread_node_allocators[tdata->tid][tdata->next_node_to_allocate++];
	rbt_node_copy(node, src, tdata);
	return node;
}

rbt_t *_rbt_new_helper()
{
	rbt_t *rbt;

	XMALLOC(rbt, 1);
	rbt->root = NULL;

	pthread_spin_init(&rbt->rbt_lock, PTHREAD_PROCESS_SHARED);

	return rbt;
}

static inline rbt_node_t *rbt_rotate_left(rbt_node_t *node)
{
	assert(node != NULL && node->right != NULL);

	rbt_node_t *node_right = node->right;

	node->right = node->right->left;
	node_right->left = node;

	return node_right;
}

static inline rbt_node_t *rbt_rotate_right(rbt_node_t *node)
{
	assert(node != NULL && node->left != NULL);

	rbt_node_t *node_left = node->left;

	node->left = node->left->right;
	node_left->right = node;

	return node_left;
}

/**
 * Traverses the tree `rbt` as dictated by `key`.
 * When returning, `leaf` is either NULL (key not found) or the leaf that
 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
 * the node that will be the parent of the inserted node.
 * In the case of an empty tree both `parent` and `leaf` are NULL.
 **/
static inline void _traverse(rbt_t *rbt, int key, rbt_node_t **parent,
                                                  rbt_node_t **leaf)
{
	*parent = NULL;
	*leaf = rbt->root;

	while (*leaf) {
		int leaf_key = (*leaf)->key;
		if (leaf_key == key)
			return;

		*parent = *leaf;
		*leaf = (key < leaf_key) ? (*leaf)->left : (*leaf)->right;
	}
}
static inline void _traverse_with_stack(rbt_t *rbt, int key,
                                        rbt_node_t *node_stack[MAX_HEIGHT],
                                        int *stack_top)
{
	rbt_node_t *parent, *leaf;

	parent = NULL;
	leaf = rbt->root;
	*stack_top = -1;

	while (leaf) {
		node_stack[++(*stack_top)] = leaf;

		int leaf_key = leaf->key;
		if (leaf_key == key)
			return;

		parent = leaf;
		leaf = (key < leaf_key) ? leaf->left : leaf->right;
	}
}

/**
 * Returns 1 if found, else 0.
 **/
int _rbt_lookup_helper(rbt_t *rbt, int key, tdata_t *tdata)
{
	rbt_node_t *parent, *leaf;
	_traverse(rbt, key, &parent, &leaf);
	return (leaf != NULL);
}

/*********************    FOR DEBUGGING ONLY    *******************************/
static void rbt_print_rec(rbt_node_t *root, int level)
{
	int i;

	if (root)
		rbt_print_rec(root->right, level + 1);

	for (i = 0; i < level; i++)
		printf("|--");

	if (!root) {
		printf("NULL\n");
		return;
	}

	printf("%d[%s][%p (%p,%p)]\n", root->key, IS_RED(root) ? "RED" : "BLA", root, 
	                               &root->left, &root->right);

	rbt_print_rec(root->left, level + 1);
}

static void rbt_print_struct(rbt_t *rbt)
{
	if (rbt->root == NULL)
		printf("[empty]");
	else
		rbt_print_rec(rbt->root, 0);
	printf("\n");
}
/******************************************************************************/

static int _insert_rebalance(rbt_t *rbt, int key, rbt_node_t *node_stack[MAX_HEIGHT],
                              int stack_top, rbt_node_t **tree_cp_root, rbt_node_t **conn_point,
                              tdata_t *tdata)
{
	rbt_node_t *parent, *grandparent, *grandgrandparent, *uncle;
	rbt_node_t *parent_cp, *grandparent_cp, *grandgrandparent_cp, *uncle_cp;

	while (1) {
		if (stack_top < 0) {
			(*tree_cp_root)->color = BLACK;
			*conn_point = NULL;
			break;
		} else if (stack_top == 0) {
			*conn_point = node_stack[stack_top];
			break;
		}

		parent = node_stack[stack_top--];
		if (IS_BLACK(parent))
			break;

		grandparent = node_stack[stack_top--];
		if (key < grandparent->key) {
			uncle = grandparent->right;

			// Copy parent and grandparent ...
			grandparent_cp = rbt_node_new_copy(grandparent, tdata);
			ht_insert(tdata->ht, &grandparent->right, uncle);
			parent_cp      = rbt_node_new_copy(parent, tdata);
			// ... and connect them with each other and with the previous copy
			grandparent_cp->left = parent_cp;
			if (key < parent->key) {
				ht_insert(tdata->ht, &parent->right, parent_cp->right);
				parent_cp->left = *tree_cp_root;
			} else {
				ht_insert(tdata->ht, &parent->left, parent_cp->left);
				parent_cp->right = *tree_cp_root;
			}
			*tree_cp_root = grandparent_cp;

			if (IS_RED(uncle)) { // CASE 1
				// Copy uncle as well
				uncle_cp              = rbt_node_new_copy(uncle, tdata);
				ht_insert(tdata->ht, &uncle->left, uncle_cp->left);
				ht_insert(tdata->ht, &uncle->right, uncle_cp->right);
				grandparent_cp->right = uncle_cp;

				parent_cp->color      = BLACK;
				uncle_cp->color       = BLACK;
				grandparent_cp->color = RED;

				if (stack_top >= 0) *conn_point = node_stack[stack_top];
				else                *conn_point = NULL;
				continue;
			}

			if (key < parent->key) { // CASE 2
				if (stack_top == -1) {
					*conn_point = NULL;
					*tree_cp_root = rbt_rotate_right(grandparent_cp);
				} else {
					grandgrandparent = node_stack[stack_top];
					*conn_point = grandgrandparent;
					*tree_cp_root = rbt_rotate_right(grandparent_cp);
				}
				parent_cp->color      = BLACK;
				grandparent_cp->color = RED;
			} else { // CASE 3
				grandparent_cp->left = rbt_rotate_left(parent_cp);
				if (stack_top == -1) {
					*conn_point = NULL;
					*tree_cp_root = rbt_rotate_right(grandparent_cp);
					(*tree_cp_root)->color = BLACK;
				} else {
					grandgrandparent = node_stack[stack_top];
					*conn_point = grandgrandparent;
					*tree_cp_root = rbt_rotate_right(grandparent_cp);
					(*tree_cp_root)->color = BLACK;
				}
				grandparent_cp->color = RED;
			}
			break;
		} else {
			uncle = grandparent->left;

			// Copy parent and grandparent ...
			grandparent_cp = rbt_node_new_copy(grandparent, tdata);
			ht_insert(tdata->ht, &grandparent->left, uncle);
			parent_cp      = rbt_node_new_copy(parent, tdata);
			// ... and connect them with each other and with the previous copy
			grandparent_cp->right = parent_cp;
			if (key < parent->key) {
				ht_insert(tdata->ht, &parent->right, parent_cp->right);
				parent_cp->left = *tree_cp_root;
			} else {
				ht_insert(tdata->ht, &parent->left, parent_cp->left);
				parent_cp->right = *tree_cp_root;
			}
			*tree_cp_root = grandparent_cp;

			if (IS_RED(uncle)) { // CASE 1
				// Copy uncle as well
				uncle_cp              = rbt_node_new_copy(uncle, tdata);
				ht_insert(tdata->ht, &uncle->left, uncle_cp->left);
				ht_insert(tdata->ht, &uncle->right, uncle_cp->right);
				grandparent_cp->left = uncle_cp;

				parent_cp->color      = BLACK;
				uncle_cp->color       = BLACK;
				grandparent_cp->color = RED;

				if (stack_top >= 0) *conn_point = node_stack[stack_top];
				else                *conn_point = NULL;
				continue;
			}

			if (key > parent->key) { // CASE 2
				if (stack_top == -1) {
					*conn_point = NULL;
					*tree_cp_root = rbt_rotate_left(grandparent_cp);
				} else {
					grandgrandparent = node_stack[stack_top];
					*conn_point = grandgrandparent;
					*tree_cp_root = rbt_rotate_left(grandparent_cp);
				}
				parent_cp->color      = BLACK;
				grandparent_cp->color = RED;
			} else { // CASE 3
				grandparent_cp->right = rbt_rotate_right(parent_cp);
				if (stack_top == -1) {
					*conn_point = NULL;
					*tree_cp_root = rbt_rotate_left(grandparent_cp);
					(*tree_cp_root)->color = BLACK;
				} else {
					grandgrandparent = node_stack[stack_top];
					*conn_point = grandgrandparent;
					*tree_cp_root = rbt_rotate_left(grandparent_cp);
					(*tree_cp_root)->color = BLACK;
				}
				grandparent_cp->color = RED;
			}
			break;
		}
	}

	return 1;
}

static int _insert(rbt_t *rbt, int key, void *data, rbt_node_t *node_stack[MAX_HEIGHT],
                   int stack_top, rbt_node_t **tree_cp_root, rbt_node_t **conn_point,
                   tdata_t *tdata)
{
	// Empty tree
	if (stack_top == -1) {
		ht_insert(tdata->ht, &rbt->root, NULL);
		*conn_point = NULL;
		*tree_cp_root = rbt_node_new(key, RED, data);
		return 1;
	}

	rbt_node_t *parent = node_stack[stack_top];
	if (key == parent->key)     return 0;

	*conn_point = parent;
	*tree_cp_root = rbt_node_new(key, RED, data);
	if (key < parent->key) ht_insert(tdata->ht, &parent->left, NULL);
	else                   ht_insert(tdata->ht, &parent->right, NULL);

	return 1;
}

static int _rbt_insert_helper(rbt_t *rbt, int key, void *data, tdata_t *tdata)
{
	rbt_node_t *tree_cp_root, *connection_point;
	rbt_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	int retries = -1;
	tm_begin_ret_t status;

try_from_scratch:

	ht_reset(tdata->ht);

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&rbt->rbt_lock);
		_traverse_with_stack(rbt, key, node_stack, &stack_top);
		int ret = _insert(rbt, key, data, node_stack, stack_top,
		                  &tree_cp_root, &connection_point, tdata);
		if (ret == 0) {
			pthread_spin_unlock(&rbt->rbt_lock);
			return 0;
		}
		ret = _insert_rebalance(rbt, key, node_stack, stack_top,
		                  &tree_cp_root, &connection_point, tdata);
		if (ret == 0) {
			pthread_spin_unlock(&rbt->rbt_lock);
			return 0;
		}

		if (!connection_point) {
			rbt->root = tree_cp_root;
		} else {
			if (key < connection_point->key) connection_point->left = tree_cp_root;
			else                             connection_point->right  = tree_cp_root;
		}

		pthread_spin_unlock(&rbt->rbt_lock);
		return 1;
	}

	// Asynchronized traversal
	_traverse_with_stack(rbt, key, node_stack, &stack_top);

	// Insert and Rebalance using copies
	int ret = _insert(rbt, key, data, node_stack, stack_top,
	                  &tree_cp_root, &connection_point, tdata);
	if (ret == 0) return 0;
	ret = _insert_rebalance(rbt, key, node_stack, stack_top,
	                  &tree_cp_root, &connection_point, tdata);
	if (ret == 0) return 0;

validate_and_connect_copy:
	/* Transactional verification. */
	while (rbt->rbt_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (rbt->rbt_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Validate copy
		int i, j;
		if (rbt->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		for (i=0; i < stack_top; i++) {
			if (key <= node_stack[i]->key) {
				if (node_stack[i]->left != node_stack[i+1])
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			} else {
				if (node_stack[i]->right!= node_stack[i+1])
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				rbt_node_t **np = tdata->ht->entries[i][j];
				rbt_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}
		if (key < node_stack[stack_top]->key && node_stack[stack_top]->left != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (key > node_stack[stack_top]->key && node_stack[stack_top]->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);


		// Install the modified copy!
		if (!connection_point) {
			rbt->root = tree_cp_root;
		} else {
			if (key < connection_point->key) connection_point->left = tree_cp_root;
			else                             connection_point->right  = tree_cp_root;
		}

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		if (ABORT_IS_EXPLICIT(status) && 
		    ABORT_CODE(status) == ABORT_VALIDATION_FAILURE) {
			tdata->tx_aborts_explicit_validation++;
			goto try_from_scratch;
		} else {
			goto validate_and_connect_copy;
		}
	}

	return 1;
}

static void _find_successor_with_stack(rbt_node_t *node_stack[MAX_HEIGHT],
                                       int *stack_top, tdata_t *tdata)
{
	rbt_node_t *l, *r;
	rbt_node_t *curr = node_stack[*stack_top];

	l = curr->left;
	r = curr->right;
	ht_insert(tdata->ht, &curr->left, l);
	ht_insert(tdata->ht, &curr->right, r);
	if (l != NULL && r != NULL) {
		curr = r;
		node_stack[++(*stack_top)] = curr;
		l = curr->left;
		ht_insert(tdata->ht, &curr->left, l);
		while (l != NULL) {
			curr = l;
			node_stack[++(*stack_top)] = curr;
			l = curr->left;
			ht_insert(tdata->ht, &curr->left, l);
		}
	}
}

static int _delete_and_rebalance(rbt_t *rbt, int key, 
                                 rbt_node_t *node_stack[MAX_HEIGHT], int stack_top,
                                 rbt_node_t **tree_cp_root, rbt_node_t **conn_point,
                                 tdata_t *tdata)
{
	int original_node_stack_index = stack_top;
	int original_key = key;
	rbt_node_t *original_node = node_stack[stack_top];
	rbt_node_t *curr, *parent, *gparent, *sibling;
	rbt_node_t *curr_cp, *parent_cp, *gparent_cp, *sibling_cp;
	rbt_node_t *new_sibling_cp;

	_find_successor_with_stack(node_stack, &stack_top, tdata);
	key = node_stack[stack_top]->key;

	// Initialize `tree_cp_root` and `conn_point`
	if (stack_top == 0) {
		*conn_point = NULL;
	} else {
		parent = node_stack[stack_top - 1];
		*conn_point = parent;
	}
	rbt_node_t *leaf = node_stack[stack_top];

	rbt_node_t *l = leaf->left, *r = leaf->right;
	*tree_cp_root = l;
	if (!l) *tree_cp_root = r;
	node_stack[stack_top] = *tree_cp_root;
	ht_insert(tdata->ht, &leaf->left, l);
	ht_insert(tdata->ht, &leaf->right, r);

	// ------------------------------------------------------------------------
	// From now on is the rebalancing
	color_t deleted_node_color = leaf->color;

	// The deleted node was RED, no rebalance necessary
	if (deleted_node_color == RED) {
		if (*conn_point) {
			if (key < (*conn_point)->key)
				ht_insert(tdata->ht, &((*conn_point)->left), leaf);
			else
				ht_insert(tdata->ht, &((*conn_point)->right), leaf);
		}
		goto replace_original_node_key;
	}
	// The replacement node was RED, just make it BLACK
	if (IS_RED(*tree_cp_root)) {
		if (*conn_point) {
			if (key < (*conn_point)->key)
				ht_insert(tdata->ht, &((*conn_point)->left), leaf);
			else
				ht_insert(tdata->ht, &((*conn_point)->right), leaf);
		}
		rbt_node_t *tmp = &(**tree_cp_root);
		*tree_cp_root = rbt_node_new_copy(*tree_cp_root, tdata);
		ht_insert(tdata->ht, &tmp->left, (*tree_cp_root)->left);
		ht_insert(tdata->ht, &tmp->right, (*tree_cp_root)->right);
		(*tree_cp_root)->color = BLACK;
		goto replace_original_node_key;
	}

	while (1) {
		curr = node_stack[stack_top--];

		// We reached the root of the tree.
		if (stack_top < 0)
			return 1;

		parent = node_stack[stack_top];
		
		if (key < parent->key) { // `curr` is left child
			sibling = parent->right;

			// Copy parent and sibling
			parent_cp  = rbt_node_new_copy(parent, tdata);
			ht_insert(tdata->ht, &parent->right, sibling);
			sibling_cp = rbt_node_new_copy(sibling, tdata);
			ht_insert(tdata->ht, &sibling->left, sibling_cp->left);
			ht_insert(tdata->ht, &sibling->right, sibling_cp->right);
			parent_cp->left = *tree_cp_root;
			parent_cp->right = sibling_cp;
			if (stack_top == original_node_stack_index)
				parent_cp->key = key;

			if (IS_RED(sibling_cp)) { // CASE 1
				sibling_cp->color = BLACK;
				parent_cp->color  = RED;
				rbt_rotate_left(parent_cp);

				new_sibling_cp = rbt_node_new_copy(parent_cp->right, tdata);
				ht_insert(tdata->ht, &parent_cp->right->left, new_sibling_cp->left);
				ht_insert(tdata->ht, &parent_cp->right->right, new_sibling_cp->right);
				parent_cp->right = new_sibling_cp;
				if (IS_BLACK(new_sibling_cp->left) && IS_BLACK(new_sibling_cp->right)) {
					parent_cp->color      = BLACK;
					new_sibling_cp->color = RED;
				} else if (IS_RED(new_sibling_cp->right)) {
					rbt_node_t *new_sibling_cp_right = new_sibling_cp->right;
					new_sibling_cp->right = rbt_node_new_copy(new_sibling_cp_right, tdata);
					ht_insert(tdata->ht, &new_sibling_cp_right->left, new_sibling_cp->right->left);
					ht_insert(tdata->ht, &new_sibling_cp_right->right, new_sibling_cp->right->right);

					new_sibling_cp->right->color = BLACK;
					new_sibling_cp->color = parent_cp->color;
					parent_cp->color = BLACK;
					sibling_cp->left = rbt_rotate_left(parent_cp);
				} else {
					rbt_node_t *new_sibling_cp_left = new_sibling_cp->left;
					new_sibling_cp->left = rbt_node_new_copy(new_sibling_cp_left, tdata);
					ht_insert(tdata->ht, &new_sibling_cp_left->left, new_sibling_cp->left->left);
					ht_insert(tdata->ht, &new_sibling_cp_left->right, new_sibling_cp->left->right);

					new_sibling_cp->left->color = parent_cp->color;
					new_sibling_cp->color = BLACK;
					parent_cp->color = BLACK;
					parent_cp->right = rbt_rotate_right(new_sibling_cp);
					sibling_cp->left = rbt_rotate_left(parent_cp);
				}

				*tree_cp_root = sibling_cp;
				*conn_point = (stack_top - 1 >= 0) ? node_stack[stack_top - 1] : NULL;

				if (*conn_point) {
					if (key < (*conn_point)->key)
						ht_insert(tdata->ht, &((*conn_point)->left), node_stack[stack_top]);
					else
						ht_insert(tdata->ht, &((*conn_point)->right), node_stack[stack_top]);
				}

				goto replace_original_node_key;
			}

			if (IS_BLACK(sibling_cp->left) && IS_BLACK(sibling_cp->right)) { // CASE 2
				sibling_cp->color = RED;

				*tree_cp_root = parent_cp;
				*conn_point = (stack_top - 1 >= 0) ? node_stack[stack_top - 1] : NULL;

				if (IS_RED(parent_cp)) {
					if (*conn_point) {
						if (key < (*conn_point)->key)
							ht_insert(tdata->ht, &((*conn_point)->left), node_stack[stack_top]);
						else
							ht_insert(tdata->ht, &((*conn_point)->right), node_stack[stack_top]);
					}

					parent_cp->color = BLACK;
					goto replace_original_node_key;
				}
				continue;
			} else {
				if (IS_RED(sibling_cp->right)) { // CASE 4
					rbt_node_t *sibling_cp_right = sibling_cp->right;
					sibling_cp->right = rbt_node_new_copy(sibling_cp_right, tdata);
					ht_insert(tdata->ht, &sibling_cp_right->left, sibling_cp->right->left);
					ht_insert(tdata->ht, &sibling_cp_right->right, sibling_cp->right->right);

					sibling_cp->right->color = BLACK;
					sibling_cp->color = parent_cp->color;
					parent_cp->color = BLACK;
					rbt_rotate_left(parent_cp);

					*tree_cp_root = sibling_cp;
					*conn_point = (stack_top - 1 >= 0) ? node_stack[stack_top - 1] : NULL;

					if (*conn_point) {
						if (key < (*conn_point)->key)
							ht_insert(tdata->ht, &((*conn_point)->left), node_stack[stack_top]);
						else
							ht_insert(tdata->ht, &((*conn_point)->right), node_stack[stack_top]);
					}
				} else { // CASE 3
					rbt_node_t *sibling_cp_left = sibling_cp->left;
					sibling_cp->left = rbt_node_new_copy(sibling_cp_left, tdata);
					ht_insert(tdata->ht, &sibling_cp_left->left, sibling_cp->left->left);
					ht_insert(tdata->ht, &sibling_cp_left->right, sibling_cp->left->right);

					sibling_cp->left->color = parent_cp->color;
					parent_cp->color = BLACK;
					sibling_cp->color = BLACK;
					parent_cp->right = rbt_rotate_right(sibling_cp);
					*tree_cp_root = rbt_rotate_left(parent_cp);

					*conn_point = (stack_top - 1 >= 0) ? node_stack[stack_top - 1] : NULL;

					if (*conn_point) {
						if (key < (*conn_point)->key)
							ht_insert(tdata->ht, &((*conn_point)->left), node_stack[stack_top]);
						else
							ht_insert(tdata->ht, &((*conn_point)->right), node_stack[stack_top]);
					}
				}

				goto replace_original_node_key;
			}
		} else { // `curr` is right child
			sibling = parent->left;

			// Copy parent and sibling
			parent_cp  = rbt_node_new_copy(parent, tdata);
			ht_insert(tdata->ht, &parent->left, sibling);
			sibling_cp = rbt_node_new_copy(sibling, tdata);
			ht_insert(tdata->ht, &sibling->left, sibling_cp->left);
			ht_insert(tdata->ht, &sibling->right, sibling_cp->right);
			parent_cp->left = sibling_cp;
			parent_cp->right = *tree_cp_root;
			if (stack_top == original_node_stack_index)
				parent_cp->key = key;

			if (IS_RED(sibling_cp)) { // CASE 1
				sibling_cp->color = BLACK;
				parent_cp->color  = RED;
				rbt_rotate_right(parent_cp);

				new_sibling_cp = rbt_node_new_copy(parent_cp->left, tdata);
				ht_insert(tdata->ht, &parent_cp->left->left, new_sibling_cp->left);
				ht_insert(tdata->ht, &parent_cp->left->right, new_sibling_cp->right);
				parent_cp->left = new_sibling_cp;
				if (IS_BLACK(new_sibling_cp->left) && IS_BLACK(new_sibling_cp->right)) {
					parent_cp->color      = BLACK;
					new_sibling_cp->color = RED;
				} else if (IS_RED(new_sibling_cp->left)) {
					rbt_node_t *new_sibling_cp_left = new_sibling_cp->left;
					new_sibling_cp->left = rbt_node_new_copy(new_sibling_cp_left, tdata);
					ht_insert(tdata->ht, &new_sibling_cp_left->left, new_sibling_cp->left->left);
					ht_insert(tdata->ht, &new_sibling_cp_left->right, new_sibling_cp->left->right);

					new_sibling_cp->left->color = BLACK;
					new_sibling_cp->color = parent_cp->color;
					parent_cp->color = BLACK;
					sibling_cp->right = rbt_rotate_right(parent_cp);
				} else {
					rbt_node_t *new_sibling_cp_right = new_sibling_cp->right;
					new_sibling_cp->right = rbt_node_new_copy(new_sibling_cp_right, tdata);
					ht_insert(tdata->ht, &new_sibling_cp_right->left, new_sibling_cp->right->left);
					ht_insert(tdata->ht, &new_sibling_cp_right->right, new_sibling_cp->right->right);

					new_sibling_cp->right->color = parent_cp->color;
					new_sibling_cp->color = BLACK;
					parent_cp->color = BLACK;
					parent_cp->left = rbt_rotate_left(new_sibling_cp);
					sibling_cp->right = rbt_rotate_right(parent_cp);
				}

				*tree_cp_root = sibling_cp;
				*conn_point = (stack_top - 1 >= 0) ? node_stack[stack_top - 1] : NULL;

				if (*conn_point) {
					if (key < (*conn_point)->key)
						ht_insert(tdata->ht, &((*conn_point)->left), node_stack[stack_top]);
					else
						ht_insert(tdata->ht, &((*conn_point)->right), node_stack[stack_top]);
				}

				goto replace_original_node_key;
			}

			if (IS_BLACK(sibling_cp->left) && IS_BLACK(sibling_cp->right)) { // CASE 2
				sibling_cp->color = RED;

				*tree_cp_root = parent_cp;
				*conn_point = (stack_top - 1 >= 0) ? node_stack[stack_top - 1] : NULL;

				if (IS_RED(parent_cp)) {
					if (*conn_point) {
						if (key < (*conn_point)->key)
							ht_insert(tdata->ht, &((*conn_point)->left), node_stack[stack_top]);
						else
							ht_insert(tdata->ht, &((*conn_point)->right), node_stack[stack_top]);
					}

					parent_cp->color = BLACK;
					goto replace_original_node_key;
				}
			} else {
				if (IS_RED(sibling_cp->left)) { // CASE 4
					rbt_node_t *sibling_cp_left = sibling_cp->left;
					sibling_cp->left = rbt_node_new_copy(sibling_cp_left, tdata);
					ht_insert(tdata->ht, &sibling_cp_left->left, sibling_cp->left->left);
					ht_insert(tdata->ht, &sibling_cp_left->right, sibling_cp->left->right);

					sibling_cp->left->color = BLACK;
					sibling_cp->color = parent_cp->color;
					parent_cp->color = BLACK;
					rbt_rotate_right(parent_cp);

					*tree_cp_root = sibling_cp;
					*conn_point = (stack_top - 1 >= 0) ? node_stack[stack_top - 1] : NULL;

					if (*conn_point) {
						if (key < (*conn_point)->key)
							ht_insert(tdata->ht, &((*conn_point)->left), node_stack[stack_top]);
						else
							ht_insert(tdata->ht, &((*conn_point)->right), node_stack[stack_top]);
					}
				} else { // CASE 3
					rbt_node_t *sibling_cp_right = sibling_cp->right;
					sibling_cp->right = rbt_node_new_copy(sibling_cp_right, tdata);
					ht_insert(tdata->ht, &sibling_cp_right->left, sibling_cp->right->left);
					ht_insert(tdata->ht, &sibling_cp_right->right, sibling_cp->right->right);

					sibling_cp->right->color = parent_cp->color;
					parent_cp->color = BLACK;
					sibling_cp->color = BLACK;
					parent_cp->left = rbt_rotate_left(sibling_cp);
					*tree_cp_root = rbt_rotate_right(parent_cp);

					*conn_point = (stack_top - 1 >= 0) ? node_stack[stack_top - 1] : NULL;

					if (*conn_point) {
						if (key < (*conn_point)->key)
							ht_insert(tdata->ht, &((*conn_point)->left), node_stack[stack_top]);
						else
							ht_insert(tdata->ht, &((*conn_point)->right), node_stack[stack_top]);
					}
				}
				goto replace_original_node_key;
			}
		}
	}

replace_original_node_key:
	if (original_node_stack_index < stack_top) {
		curr_cp = NULL;
		int i;
		for (i=stack_top-1; i >= original_node_stack_index; i--) {
			curr_cp = rbt_node_new_copy(node_stack[i], tdata);
			ht_insert(tdata->ht, &node_stack[i]->left, curr_cp->left);
			ht_insert(tdata->ht, &node_stack[i]->right, curr_cp->right);
			if (key < curr_cp->key) curr_cp->left = *tree_cp_root;
			else                    curr_cp->right = *tree_cp_root;
			*tree_cp_root = curr_cp;
			*conn_point = (i - 1 >= 0) ? node_stack[i-1] : NULL;

			if (*conn_point) {
				if (key < (*conn_point)->key)
					ht_insert(tdata->ht, &((*conn_point)->left), node_stack[i]);
				else
					ht_insert(tdata->ht, &((*conn_point)->right), node_stack[i]);
			}
		}
		curr_cp->key = key;
	}

	return 1;
}

static int _rbt_delete_helper(rbt_t *rbt, int key, tdata_t *tdata)
{
	rbt_node_t *tree_cp_root, *connection_point;
	rbt_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	color_t deleted_node_color;
	int succ_key, original_node_stack_index;
	int retries = -1;
	tm_begin_ret_t status;

try_from_scratch:

	ht_reset(tdata->ht);

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&rbt->rbt_lock);
		_traverse_with_stack(rbt, key, node_stack, &stack_top);
		if (stack_top < 0 || node_stack[stack_top]->key != key) {
			pthread_spin_unlock(&rbt->rbt_lock);
			return 0;
		}
		int ret = _delete_and_rebalance(rbt, key, node_stack, stack_top,
		                                &tree_cp_root, &connection_point,
		                                tdata);
		if (ret == 0) {
			pthread_spin_unlock(&rbt->rbt_lock);
			return 0;
		}
		if (!connection_point) {
			rbt->root = tree_cp_root;
		} else {
			if (key < connection_point->key) connection_point->left  = tree_cp_root;
			else                             connection_point->right = tree_cp_root;
		}
		pthread_spin_unlock(&rbt->rbt_lock);
		return 1;
	}

	// Asynchronized traversal
	_traverse_with_stack(rbt, key, node_stack, &stack_top);
	if (stack_top < 0 || node_stack[stack_top]->key != key)
		return 0;

	int ret = _delete_and_rebalance(rbt, key, node_stack, stack_top,
	                                &tree_cp_root, &connection_point,
	                                tdata);
	if (ret == 0) return 0;

validate_and_connect_copy:
	/* Transactional verification. */
	while (rbt->rbt_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (rbt->rbt_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// FIXME Validate copy
		int i, j;
		if (rbt->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		for (i=0; i < stack_top-1; i++) {
			if (key < node_stack[i]->key) {
				if (node_stack[i]->left != node_stack[i+1])
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			} else {
				if (node_stack[i]->right != node_stack[i+1])
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				rbt_node_t **np = tdata->ht->entries[i][j];
				rbt_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

		// Install the modified copy!
		if (!connection_point) {
			rbt->root = tree_cp_root;
		} else {
			if (key < connection_point->key) connection_point->left = tree_cp_root;
			else                             connection_point->right  = tree_cp_root;
		}

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		if (ABORT_IS_EXPLICIT(status) && 
		    ABORT_CODE(status) == ABORT_VALIDATION_FAILURE) {
			tdata->tx_aborts_explicit_validation++;
			goto try_from_scratch;
		} else {
			goto validate_and_connect_copy;
		}
	}

	return 1;
}

static int key_in_min_path, key_in_max_path;
static int bh;
static int paths_with_bh_diff;
static int total_paths;
static int min_path_len, max_path_len;
static int total_nodes, red_nodes, black_nodes;
static int red_red_violations, bst_violations;
static void _rbt_validate(rbt_node_t *root, int _bh, int _th)
{
	if (!root)
		return;

	rbt_node_t *left = root->left;
	rbt_node_t *right = root->right;

	total_nodes++;
	black_nodes += (IS_BLACK(root));
	red_nodes += (IS_RED(root));
	_th++;
	_bh += (IS_BLACK(root));

	/* BST violation? */
	if (left && left->key >= root->key)
		bst_violations++;
	if (right && right->key <= root->key)
		bst_violations++;

	/* Red-Red violation? */
	if (IS_RED(root) && (IS_RED(left) || IS_RED(right)))
		red_red_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if (!left || !right) {
		total_paths++;
		if (bh == -1)
			bh = _bh;
		else if (_bh != bh)
			paths_with_bh_diff++;

		if (_th <= min_path_len) {
			min_path_len = _th;
			key_in_min_path = root->key;
		}
		if (_th >= max_path_len) {
			max_path_len = _th;
			key_in_max_path = root->key;
		}
	}

	/* Check subtrees. */
	if (left)
		_rbt_validate(left, _bh, _th);
	if (right)
		_rbt_validate(right, _bh, _th);
}

static inline int _rbt_validate_helper(rbt_node_t *root)
{
	int check_bh = 0, check_red_red = 0, check_bst = 0;
	int check_rbt = 0;
	bh = -1;
	paths_with_bh_diff = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = black_nodes = red_nodes = 0;
	red_red_violations = 0;
	bst_violations = 0;

	_rbt_validate(root, 0, 0);

	check_bh = (paths_with_bh_diff == 0);
	check_red_red = (red_red_violations == 0);
	check_bst = (bst_violations == 0);
	check_rbt = (check_bh && check_red_red && check_bst);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  Valid Red-Black Tree: %s\n",
	       check_rbt ? "Yes [OK]" : "No [ERROR]");
	printf("  Black height: %d [%s]\n", bh,
	       check_bh ? "OK" : "ERROR");
	printf("  Red-Red Violation: %s\n",
	       check_red_red ? "No [OK]" : "Yes [ERROR]");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size (Total / Black / Red): %8d / %8d / %8d\n",
	       total_nodes, black_nodes, red_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("  Key in min path: %d\n", key_in_min_path);
	printf("  Key in max path: %d\n", key_in_max_path);
	printf("\n");

	return check_rbt;
}

static void _insert_rebalance_warmup(rbt_t *rbt, int key,
                    rbt_node_t *node_stack[MAX_HEIGHT], int stack_top)
{
	rbt_node_t *parent, *grandparent, *grandgrandparent, *uncle;

	while (1) {
		if (stack_top <= 0) {
			rbt->root->color = BLACK;
			break;
		}

		parent = node_stack[stack_top--];
		if (IS_BLACK(parent))
			break;

		grandparent = node_stack[stack_top--];
		if (key < grandparent->key) {
			uncle = grandparent->right;
			if (IS_RED(uncle)) {
				parent->color = BLACK;
				uncle->color = BLACK;
				grandparent->color = RED;
				continue;
			}

			if (key < parent->key) {
				if (stack_top == -1) {
					rbt->root = rbt_rotate_right(grandparent);
				} else {
					grandgrandparent = node_stack[stack_top];
					if (key < grandgrandparent->key)
						grandgrandparent->left = rbt_rotate_right(grandparent);
					else
						grandgrandparent->right = rbt_rotate_right(grandparent);
				}
				parent->color = BLACK;
				grandparent->color = RED;
			} else {
				grandparent->left = rbt_rotate_left(parent);
				if (stack_top == -1) {
					rbt->root = rbt_rotate_right(grandparent);
					rbt->root->color = BLACK;
				} else {
					grandgrandparent = node_stack[stack_top];
					if (key < grandgrandparent->key) {
						grandgrandparent->left = rbt_rotate_right(grandparent);
						grandgrandparent->left->color = BLACK;
					} else {
						grandgrandparent->right = rbt_rotate_right(grandparent);
						grandgrandparent->right->color = BLACK;
					}
				}
				grandparent->color = RED;
			}
			break;
		} else {
			uncle = grandparent->left;
			if (IS_RED(uncle)) {
				parent->color = BLACK;
				uncle->color = BLACK;
				grandparent->color = RED;
				continue;
			}

			if (key > parent->key) {
				if (stack_top == -1) {
					rbt->root = rbt_rotate_left(grandparent);
				} else {
					grandgrandparent = node_stack[stack_top];
					if (key < grandgrandparent->key)
						grandgrandparent->left = rbt_rotate_left(grandparent);
					else
						grandgrandparent->right = rbt_rotate_left(grandparent);
				}
				parent->color = BLACK;
				grandparent->color = RED;
			} else {
				grandparent->right = rbt_rotate_right(parent);
				if (stack_top == -1) {
					rbt->root = rbt_rotate_left(grandparent);
					rbt->root->color = BLACK;
				} else {
					grandgrandparent = node_stack[stack_top];
					if (key < grandgrandparent->key) {
						grandgrandparent->left = rbt_rotate_left(grandparent);
						grandgrandparent->left->color = BLACK;
					} else {
						grandgrandparent->right = rbt_rotate_left(grandparent);
						grandgrandparent->right->color = BLACK;
					}
				}
				grandparent->color = RED;
			}
			break;
		}
	}
}

static int _insert_warmup(rbt_t *rbt, int key, void *data,
                   rbt_node_t *node_stack[MAX_HEIGHT], int stack_top)
{
	// Empty tree
	if (stack_top == -1) {
		rbt->root = rbt_node_new(key, RED, data);
		return 1;
	}

	rbt_node_t *parent = node_stack[stack_top];
	if (key == parent->key)     return 0;
	else if (key < parent->key) parent->left = rbt_node_new(key, RED, data);
	else                        parent->right = rbt_node_new(key, RED, data);
	return 1;
}

static int _rbt_insert_helper_warmup(rbt_t *rbt, int key, void *data)
{
	rbt_node_t *node_stack[MAX_HEIGHT];
	int stack_top;

	_traverse_with_stack(rbt, key, node_stack, &stack_top);
	int ret = _insert_warmup(rbt, key, data, node_stack, stack_top);
	if (ret == 0) return 0;

	_insert_rebalance_warmup(rbt, key, node_stack, stack_top);
	return 1;
}

static inline int _rbt_warmup_helper(rbt_t *rbt, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;
		ret = _rbt_insert_helper_warmup(rbt, key, NULL);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}


/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
	printf("Size of tree node is %lu\n", sizeof(rbt_node_t));
	return _rbt_new_helper();
}

void *rbt_thread_data_new(int tid)
{
	// Pre allocate a large amount of nodes for each thread
	per_thread_node_allocators[tid] = malloc(10000000*sizeof(rbt_node_t));
	memset(per_thread_node_allocators[tid], 0, 10000000*sizeof(rbt_node_t));

	tdata_t *tdata = tdata_new(tid);

	return tdata;
}

void rbt_thread_data_print(void *thread_data)
{
	tdata_t *tdata = thread_data;
	tdata_print(tdata);
	return;
}

void rbt_thread_data_add(void *d1, void *d2, void *dst)
{
	tdata_add(d1, d2, dst);
}

int rbt_lookup(void *rbt, void *thread_data, int key)
{
	int ret = 0;
	tdata_t *tdata = thread_data;

	ret = _rbt_lookup_helper(rbt, key, tdata);

	return ret; 
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret = 0;
	tdata_t *tdata = thread_data;

	ret = _rbt_insert_helper(rbt, key, value, tdata);

	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret = 0;
	tdata_t *tdata = thread_data;
	ret = _rbt_delete_helper(rbt, key, tdata);
	return ret;
}

int rbt_validate(void *rbt)
{
	int ret = 0;
	ret = _rbt_validate_helper(((rbt_t *)rbt)->root);
	return ret;
}

int rbt_warmup(void *rbt, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _rbt_warmup_helper((rbt_t *)rbt, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "rbt-rcu-htm-internal";
}
