/**
 * An internal AVL tree.
 **/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>  //> memset() for per thread allocator

#include "alloc.h"
#include "arch.h"

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

void *ht_get(ht_t *ht, void *key)
{
	int bucket = HT_GET_BUCKET(key);
	int i;

	for (i=0; i < ht->bucket_next_index[bucket]; i+=2)
		if (key == ht->entries[bucket][i])
			return ht->entries[bucket][i+1];

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

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )
#define MAX_HEIGHT 50

typedef struct avl_node_s {
	int key;
	void *data;

	int height;

	struct avl_node_s *left,
	                  *right;
} __attribute__((packed)) avl_node_t;

typedef struct {
	avl_node_t *root;

	// Add extra padding here to avoid having `root` in the
	// same cache line with the lock.
	char padding[CACHE_LINE_SIZE - sizeof(avl_node_t *)];

	pthread_spinlock_t avl_lock;
} avl_t;

#define NODES_PER_ALLOCATOR 10000000
avl_node_t *per_thread_node_allocators[88];

static avl_node_t *avl_node_new(int key, void *data)
{
	avl_node_t *node;

	XMALLOC(node, 1);
	node->key = key;
	node->data = data;
	node->height = 0; // new nodes have height 0 and NULL has height -1.
	node->right = node->left = NULL;
	return node;
}

static void avl_node_copy(avl_node_t *dest, avl_node_t *src)
{
	dest->key = src->key;
	dest->data = src->data;
	dest->height = src->height;
	dest->left = src->left;
	dest->right = src->right;
	__sync_synchronize();
}

static avl_node_t *avl_node_new_copy(avl_node_t *src, tdata_t *tdata)
{
	avl_node_t *node;
	if (tdata->next_node_to_allocate >= NODES_PER_ALLOCATOR)
		node = avl_node_new(0, NULL);
	else
		node = &per_thread_node_allocators[tdata->tid][tdata->next_node_to_allocate++];
	avl_node_copy(node, src);
	return node;
}

static inline int node_height(avl_node_t *n)
{
	if (!n)
		return -1;
	else
		return n->height;
}

static inline int node_balance(avl_node_t *n)
{
	if (!n)
		return 0;

	return node_height(n->left) - node_height(n->right);
}

static avl_t *_avl_new_helper()
{
	avl_t *avl;

	XMALLOC(avl, 1);
	avl->root = NULL;
	pthread_spin_init(&avl->avl_lock, PTHREAD_PROCESS_SHARED);

	return avl;
}

static inline avl_node_t *rotate_right(avl_node_t *node)
{
	assert(node != NULL && node->left != NULL);

	avl_node_t *node_left = node->left;

	node->left = node->left->right;
	node_left->right = node;

	node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
	node_left->height = MAX(node_height(node_left->left), node_height(node_left->right)) + 1;
	return node_left;
}
static inline avl_node_t *rotate_left(avl_node_t *node)
{
	assert(node != NULL && node->right != NULL);

	avl_node_t *node_right = node->right;

	node->right = node->right->left;
	node_right->left = node;

	node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
	node_right->height = MAX(node_height(node_right->left), node_height(node_right->right)) + 1;
	return node_right;
}

/**
 * Traverses the tree `avl` as dictated by `key`.
 * When returning, `leaf` is either NULL (key not found) or the leaf that
 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
 * the node that will be the parent of the inserted node.
 * In the case of an empty tree both `parent` and `leaf` are NULL.
 **/
static inline void _traverse(avl_t *avl, int key, avl_node_t **parent,
                                                 avl_node_t **leaf)
{
	*parent = NULL;
	*leaf = avl->root;

	while (*leaf) {
		int leaf_key = (*leaf)->key;
		if (leaf_key == key)
			return;

		*parent = *leaf;
		*leaf = (key < leaf_key) ? (*leaf)->left : (*leaf)->right;
	}
}
static inline void _traverse_with_stack(avl_t *avl, int key,
                                        avl_node_t *node_stack[MAX_HEIGHT],
                                        int *stack_top)
{
	avl_node_t *parent, *leaf;

	parent = NULL;
	leaf = avl->root;
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

static int _avl_lookup_helper(avl_t *avl, int key)
{
	avl_node_t *parent, *leaf;

	_traverse(avl, key, &parent, &leaf);
	return (leaf != NULL);
}

static inline void _avl_insert_fixup(avl_t *avl, int key,
                                     avl_node_t *node_stack[MAX_HEIGHT],
                                     int top)
{
	avl_node_t *curr, *parent;

	while (top >= 0) {
		curr = node_stack[top--];

		parent = NULL;
		if (top >= 0) parent = node_stack[top];

		int balance = node_balance(curr);
		if (balance == 2) {
			int balance2 = node_balance(curr->left);

			if (balance2 == 1) { // LEFT-LEFT case
				if (!parent)                avl->root = rotate_right(curr);
				else if (key < parent->key) parent->left = rotate_right(curr);
				else                        parent->right = rotate_right(curr);
			} else if (balance2 == -1) { // LEFT-RIGHT case
				curr->left = rotate_left(curr->left);
				if (!parent)                avl->root = rotate_right(curr); 
				else if (key < parent->key) parent->left = rotate_right(curr);
				else                        parent->right = rotate_right(curr);
			} else {
				assert(0);
			}

			break;
		} else if (balance == -2) {
			int balance2 = node_balance(curr->right);

			if (balance2 == -1) { // RIGHT-RIGHT case
				if (!parent)                avl->root = rotate_left(curr);
				else if (key < parent->key) parent->left = rotate_left(curr);
				else                        parent->right = rotate_left(curr);
			} else if (balance2 == 1) { // RIGHT-LEFT case
				curr->right = rotate_right(curr->right);
				if (!parent)                avl->root = rotate_left(curr);
				else if (key < parent->key) parent->left = rotate_left(curr);
				else                        parent->right = rotate_left(curr);
			} else {
				assert(0);
			}

			break;
		}

		/* Update the height of current node. */
		int height_saved = node_height(curr);
		int height_new = MAX(node_height(curr->left), node_height(curr->right)) + 1;
		curr->height = height_new;
		if (height_saved == height_new)
			break;
	}
}

static inline int _insert(avl_t *avl, int key, void *value,
                          avl_node_t *node_stack[MAX_HEIGHT], int stack_top)
{
	// Empty tree case
	if (stack_top < 0) {
		avl->root = avl_node_new(key, value);
		return 1;
	}

	avl_node_t *place = node_stack[stack_top];

	// Key already in the tree.
	if (place->key == key)
		return 0;

	if (key < place->key)
		place->left = avl_node_new(key, value);
	else
		place->right = avl_node_new(key, value);

	return 1;
}

static avl_node_t *_insert_and_rebalance_with_copy(int key, void *value,
        avl_node_t *node_stack[MAX_HEIGHT], int stack_top, tdata_t *tdata,
        avl_node_t **tree_copy_root_ret, int *connection_point_stack_index)
{
	avl_node_t *tree_copy_root, *connection_point;

	/* Start the tree copying with the new node. */
	tree_copy_root = avl_node_new(key, value);
	*connection_point_stack_index = stack_top;
	connection_point = node_stack[stack_top--];

	while (stack_top >= -1) {
		// If we've reached and passed root return.
		if (!connection_point)
			break;

		// If no height change occurs we can break.
		if (tree_copy_root->height + 1 <= connection_point->height)
			break;

		// Copy the current node and link it to the local copy.
		avl_node_t *curr_cp = avl_node_new_copy(connection_point, tdata);
		ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
		ht_insert(tdata->ht, &connection_point->right, curr_cp->right);

		curr_cp->height = tree_copy_root->height + 1;
		if (key < curr_cp->key) curr_cp->left = tree_copy_root;
		else                    curr_cp->right = tree_copy_root;
		tree_copy_root = curr_cp;

		// Move one level up
		*connection_point_stack_index = stack_top;
		connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;

		// Get current node's balance
		avl_node_t *sibling;
		int curr_balance;
		if (key < curr_cp->key) {
			sibling = curr_cp->right;
			curr_balance = node_height(curr_cp->left) - node_height(sibling);
		} else {
			sibling = curr_cp->left;
			curr_balance = node_height(sibling) - node_height(curr_cp->right);
		}

		if (curr_balance == 2) {
			int balance2 = node_balance(tree_copy_root->left);

			if (balance2 == 1) { // LEFT-LEFT case
				tree_copy_root = rotate_right(tree_copy_root);
			} else if (balance2 == -1) { // LEFT-RIGHT case
				tree_copy_root->left = rotate_left(tree_copy_root->left);
				tree_copy_root = rotate_right(tree_copy_root);
			} else {
				assert(0);
			}

			break;
		} else if (curr_balance == -2) {
			int balance2 = node_balance(tree_copy_root->right);

			if (balance2 == -1) { // RIGHT-RIGHT case
				tree_copy_root = rotate_left(tree_copy_root);
			} else if (balance2 == 1) { // RIGHT-LEFT case
				tree_copy_root->right = rotate_right(tree_copy_root->right);
				tree_copy_root = rotate_left(tree_copy_root);
			} else {
				assert(0);
			}

			break;
		}
	}

	*tree_copy_root_ret = tree_copy_root;
	return connection_point;
}

static int _avl_insert_helper(avl_t *avl, int key, void *value, tdata_t *tdata)
{
	avl_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	tm_begin_ret_t status;
	int retries = -1;
	int i;
	avl_node_t *tree_copy_root, *connection_point;
	int connection_point_stack_index;

try_from_scratch:

	ht_reset(tdata->ht);

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&avl->avl_lock);
		_traverse_with_stack(avl, key, node_stack, &stack_top);
		if (stack_top >= 0 && node_stack[stack_top]->key == key) {
			pthread_spin_unlock(&avl->avl_lock);
			return 0;
		}
		connection_point_stack_index = -1;
		connection_point = _insert_and_rebalance_with_copy(key, value,
		                           node_stack, stack_top, tdata, &tree_copy_root,
		                           &connection_point_stack_index);
		if (!connection_point) {
			avl->root = tree_copy_root;
		} else {
			if (key <= connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}
		pthread_spin_unlock(&avl->avl_lock);
		return 1;
	}

	/* Asynchronized traversal. If key is not there we can safely return. */
	_traverse_with_stack(avl, key, node_stack, &stack_top);
	if (stack_top >= 0 && node_stack[stack_top]->key == key)
		return 0;

	// For now let's ignore empty tree case and case with only one node in the tree.
	assert(stack_top >= 2);

	connection_point_stack_index = -1;
	connection_point = _insert_and_rebalance_with_copy(key, value,
	                             node_stack, stack_top, tdata, &tree_copy_root,
	                             &connection_point_stack_index);

validate_and_connect_copy:
	/* Transactional verification. */
	while (avl->avl_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (avl->avl_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Validate copy
		if (key < node_stack[stack_top]->key && node_stack[stack_top]->left != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (key > node_stack[stack_top]->key && node_stack[stack_top]->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (avl->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		if (connection_point_stack_index <= 0) {
			for (i=0; i < stack_top; i++) {
				if (key <= node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		} else {
			avl_node_t *curr = avl->root;
			while (curr && curr != connection_point)
				curr = (key <= curr->key) ? curr->left : curr->right;
			if (curr != connection_point)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			for (i=connection_point_stack_index; i < stack_top; i++) {
				if (key <= node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		}
	
		int j;
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				avl_node_t **np = tdata->ht->entries[i][j];
				avl_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}


		// Now let's 'commit' the tree copy onto the original tree.
		if (!connection_point) {
			avl->root = tree_copy_root;
		} else {
			if (key <= connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
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

static int _avl_insert_helper_warmup(avl_t *avl, int key, void *value)
{
	avl_node_t *node_stack[MAX_HEIGHT];
	int stack_top;

	_traverse_with_stack(avl, key, node_stack, &stack_top);
	int ret = _insert(avl, key, value, node_stack, stack_top);
	if (!ret) return 0;
	_avl_insert_fixup(avl, key, node_stack, stack_top);
	return 1;
}

static inline void _find_successor_with_stack(avl_node_t *node,
                                              avl_node_t *node_stack[MAX_HEIGHT],
                                              int *stack_top, tdata_t *tdata)
{
	avl_node_t *parent, *leaf, *l, *r;

	l = node->left;
	r = node->right;
	ht_insert(tdata->ht, &node->left, l);
	ht_insert(tdata->ht, &node->right, r);
	if (!l || !r)
		return;

	parent = node;
	leaf = r;
	node_stack[++(*stack_top)] = leaf;

	while ((l = leaf->left) != NULL) {
		parent = leaf;
		leaf = l;
		node_stack[++(*stack_top)] = leaf;
	}
}

static avl_node_t *_delete_and_rebalance_with_copy(int key,
                       avl_node_t *node_stack[MAX_HEIGHT], int stack_top,
                       tdata_t *tdata, avl_node_t **tree_copy_root_ret,
                       int *connection_point_stack_index, int *new_stack_top)
{
	avl_node_t *tree_copy_root, *connection_point;
	int to_be_deleted_stack_index = stack_top;
	avl_node_t *original_to_be_deleted = node_stack[stack_top];
	avl_node_t *l, *r;

	// If needed move on to the node to be deleted.
	_find_successor_with_stack(original_to_be_deleted, node_stack, &stack_top, tdata);
	*new_stack_top = stack_top;

	avl_node_t *to_be_deleted = node_stack[stack_top];
	l = to_be_deleted->left; r = to_be_deleted->right;
	ht_insert(tdata->ht, &to_be_deleted->left, l);
	ht_insert(tdata->ht, &to_be_deleted->right, r);
	tree_copy_root = (l != NULL) ? l : r;
	stack_top--;
	*connection_point_stack_index = stack_top;
	connection_point = node_stack[stack_top--];

	while (stack_top >= -1) {
		// If we've reached and passed root return.
		if (!connection_point)
			break;

		avl_node_t *sibling;
		int curr_balance;
		if (key < connection_point->key) {
			sibling = connection_point->right;
			ht_insert(tdata->ht, &connection_point->right, sibling);
			curr_balance = node_height(tree_copy_root) - node_height(sibling);
		} else {
			sibling = connection_point->left;
			ht_insert(tdata->ht, &connection_point->left, sibling);
			curr_balance = node_height(sibling) - node_height(tree_copy_root);
		}

		// Check if rotation(s) is(are) necessary.
		if (curr_balance == 2) {
			avl_node_t *curr_cp = avl_node_new_copy(connection_point, tdata);
			curr_cp->left = sibling;

			ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
			ht_insert(tdata->ht, &connection_point->right, curr_cp->right);
			if (key < curr_cp->key) curr_cp->left = tree_copy_root;
			else                    curr_cp->right = tree_copy_root;
			tree_copy_root = curr_cp;
			if (connection_point == original_to_be_deleted)
				tree_copy_root->key = to_be_deleted->key;
			curr_cp = avl_node_new_copy(tree_copy_root->left, tdata);
			ht_insert(tdata->ht, &tree_copy_root->left->left, curr_cp->left);
			ht_insert(tdata->ht, &tree_copy_root->left->right, curr_cp->right);
			tree_copy_root->left = curr_cp;

			int balance2 = node_balance(tree_copy_root->left);

			if (balance2 == 0 || balance2 == 1) { // LEFT-LEFT case
				tree_copy_root = rotate_right(tree_copy_root);
			} else if (balance2 == -1) { // LEFT-RIGHT case
				curr_cp = avl_node_new_copy(tree_copy_root->left->right, tdata);
				ht_insert(tdata->ht, &tree_copy_root->left->right->left, curr_cp->left);
				ht_insert(tdata->ht, &tree_copy_root->left->right->right, curr_cp->right);
				tree_copy_root->left->right = curr_cp;

				tree_copy_root->left = rotate_left(tree_copy_root->left);
				tree_copy_root = rotate_right(tree_copy_root);
			} else {
				assert(0);
			}

			// Move one level up
			*connection_point_stack_index = stack_top;
			connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
			continue;
		} else if (curr_balance == -2) {
			avl_node_t *curr_cp = avl_node_new_copy(connection_point, tdata);
			curr_cp->right = sibling;

			ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
			ht_insert(tdata->ht, &connection_point->right, curr_cp->right);
			if (key < curr_cp->key) curr_cp->left = tree_copy_root;
			else                    curr_cp->right = tree_copy_root;
			tree_copy_root = curr_cp;
			if (connection_point == original_to_be_deleted)
				tree_copy_root->key = to_be_deleted->key;
			curr_cp = avl_node_new_copy(tree_copy_root->right, tdata);
			ht_insert(tdata->ht, &tree_copy_root->right->left, curr_cp->left);
			ht_insert(tdata->ht, &tree_copy_root->right->right, curr_cp->right);
			tree_copy_root->right = curr_cp;

			int balance2 = node_balance(tree_copy_root->right);

			if (balance2 == 0 || balance2 == -1) { // RIGHT-RIGHT case
				tree_copy_root = rotate_left(tree_copy_root);
			} else if (balance2 == 1) { // RIGHT-LEFT case
				curr_cp = avl_node_new_copy(tree_copy_root->right->left, tdata);
				ht_insert(tdata->ht, &tree_copy_root->right->left->left, curr_cp->left);
				ht_insert(tdata->ht, &tree_copy_root->right->left->right, curr_cp->right);
				tree_copy_root->right->left = curr_cp;
				tree_copy_root->right = rotate_right(tree_copy_root->right);
				tree_copy_root = rotate_left(tree_copy_root);
			} else {
				assert(0);
			}

			// Move one level up
			*connection_point_stack_index = stack_top;
			connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
			continue;
		}

		// Check whether current node's height is to change.
		int old_height = connection_point->height;
		int new_height = MAX(node_height(tree_copy_root), node_height(sibling)) + 1;
		if (old_height == new_height)
			break;

		// Copy the current node and link it to the local copy.
		avl_node_t *curr_cp = avl_node_new_copy(connection_point, tdata);
		if (key < curr_cp->key) curr_cp->right = sibling;
		else                    curr_cp->left = sibling;

		ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
		ht_insert(tdata->ht, &connection_point->right, curr_cp->right);

		// Change the height of current node's copy + the key if needed.
		curr_cp->height = new_height;
		if (key < curr_cp->key) curr_cp->left = tree_copy_root;
		else                    curr_cp->right = tree_copy_root;
		tree_copy_root = curr_cp;
		if (connection_point == original_to_be_deleted)
			tree_copy_root->key = to_be_deleted->key;

		// Move one level up
		*connection_point_stack_index = stack_top;
		connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;

	}

	// We may need to copy the access path from the originally deleted node
	// up to the current connection_point.
	if (to_be_deleted_stack_index <= *connection_point_stack_index) {
		int i;
		for (i=*connection_point_stack_index; i >= to_be_deleted_stack_index; i--) {
			avl_node_t *curr_cp = avl_node_new_copy(node_stack[i], tdata);
			ht_insert(tdata->ht, &node_stack[i]->left, curr_cp->left);
			ht_insert(tdata->ht, &node_stack[i]->right, curr_cp->right);

			if (key < curr_cp->key) curr_cp->left = tree_copy_root;
			else                    curr_cp->right = tree_copy_root;
			tree_copy_root = curr_cp;
		}
		tree_copy_root->key = to_be_deleted->key;
		connection_point = to_be_deleted_stack_index > 0 ? 
		                             node_stack[to_be_deleted_stack_index - 1] :
		                             NULL;
		*connection_point_stack_index = to_be_deleted_stack_index - 1;
	}

	*tree_copy_root_ret = tree_copy_root;
	return connection_point;
}

static int _avl_delete_helper(avl_t *avl, int key, tdata_t *tdata)
{
	avl_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	tm_begin_ret_t status;
	int retries = -1;
	int i;
	avl_node_t *tree_copy_root, *connection_point;
	int connection_point_stack_index;

try_from_scratch:

	ht_reset(tdata->ht);

	/* Global lock fallback.*/
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&avl->avl_lock);
		_traverse_with_stack(avl, key, node_stack, &stack_top);
		if (stack_top >= 0 && node_stack[stack_top]->key != key) {
			pthread_spin_unlock(&avl->avl_lock);
			return 0;
		}
		connection_point_stack_index = -1;
		connection_point = _delete_and_rebalance_with_copy(key,
		                           node_stack, stack_top, tdata,
		                           &tree_copy_root, &connection_point_stack_index, &stack_top);
		if (!connection_point) {
			avl->root = tree_copy_root;
		} else {
			if (key <= connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}

		pthread_spin_unlock(&avl->avl_lock);
		return 1;
	}

	/* Asynchronized traversal. If key is not there we can safely return. */
	_traverse_with_stack(avl, key, node_stack, &stack_top);
	if (stack_top >= 0 && node_stack[stack_top]->key != key)
		return 0;

	connection_point_stack_index = -1;
	connection_point = _delete_and_rebalance_with_copy(key,
	                             node_stack, stack_top, tdata,
	                             &tree_copy_root, &connection_point_stack_index, &stack_top);

validate_and_connect_copy:
	/* Transactional verification. */
	while (avl->avl_lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (avl->avl_lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Validate copy
		if (node_stack[stack_top]->left != NULL && node_stack[stack_top]->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (avl->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		if (connection_point_stack_index <= 0) {
			for (i=0; i < stack_top; i++) {
				if (key < node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		} else {
			avl_node_t *curr = avl->root;
			while (curr && curr != connection_point)
				curr = (key <= curr->key) ? curr->left : curr->right;
			if (curr != connection_point)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			for (i=connection_point_stack_index; i < stack_top; i++) {
				if (key < node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		}
	
		int j;
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				avl_node_t **np = tdata->ht->entries[i][j];
				avl_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}


		// Now let's 'commit' the tree copy onto the original tree.
		if (!connection_point) {
			avl->root = tree_copy_root;
		} else {
			if (key <= connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
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

static inline int _avl_warmup_helper(avl_t *avl, int nr_nodes, int max_key,
                                     unsigned int seed, int force)
{
	int i = 0, nodes_inserted = 0, ret = 0;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		int key = rand() % max_key;

		ret = _avl_insert_helper_warmup(avl, key, NULL);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}

static int total_paths, total_nodes, bst_violations, avl_violations;
static int min_path_len, max_path_len;
static void _avl_validate_rec(avl_node_t *root, int _th)
{
	if (!root)
		return;

	avl_node_t *left = root->left;
	avl_node_t *right = root->right;

	total_nodes++;
	_th++;

	/* BST violation? */
	if (left && left->key >= root->key)
		bst_violations++;
	if (right && right->key <= root->key)
		bst_violations++;

	/* AVL violation? */
	int balance = node_balance(root);
	if (balance < -1 || balance > 1)
		avl_violations++;

	/* We found a path (a node with at least one NULL child). */
	if (!left || !right) {
		total_paths++;

		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* Check subtrees. */
	if (left)
		_avl_validate_rec(left, _th);
	if (right)
		_avl_validate_rec(right, _th);
}

static inline int _avl_validate_helper(avl_node_t *root)
{
	int check_bst = 0, check_avl = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	bst_violations = 0;
	avl_violations = 0;

	_avl_validate_rec(root, 0);

	check_bst = (bst_violations == 0);
	check_avl = (avl_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  AVL Violation: %s\n",
	       check_avl ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size: %8d\n", total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_bst && check_avl;
}

/*********************    FOR DEBUGGING ONLY    *******************************/
static void avl_print_rec(avl_node_t *root, int level)
{
	int i;

	if (root)
		avl_print_rec(root->right, level + 1);

	for (i = 0; i < level; i++)
		printf("|--");

	if (!root) {
		printf("NULL\n");
		return;
	}

	printf("%d [%d]\n", root->key, root->height);

	avl_print_rec(root->left, level + 1);
}

static void avl_print_struct(avl_t *avl)
{
	if (avl->root == NULL)
		printf("[empty]");
	else
		avl_print_rec(avl->root, 0);
	printf("\n");
}
/******************************************************************************/

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *rbt_new()
{
	printf("Size of tree node is %lu\n", sizeof(avl_node_t));
	return _avl_new_helper();
}

void *rbt_thread_data_new(int tid)
{
	// Pre allocate a large amount of nodes for each thread
	per_thread_node_allocators[tid] = malloc(NODES_PER_ALLOCATOR*sizeof(avl_node_t));
	memset(per_thread_node_allocators[tid], 0, NODES_PER_ALLOCATOR*sizeof(avl_node_t));

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
	ret = _avl_lookup_helper(rbt, key);
	return ret; 
}

int rbt_insert(void *rbt, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = _avl_insert_helper(rbt, key, value, thread_data);
	return ret;
}

int rbt_delete(void *rbt, void *thread_data, int key)
{
	int ret = 0;
	ret = _avl_delete_helper(rbt, key, thread_data);
	return ret;
}

int rbt_validate(void *rbt)
{
	int ret = 0;
	ret = _avl_validate_helper(((avl_t *)rbt)->root);
	return ret;
}

int rbt_warmup(void *rbt, int nr_nodes, int max_key, 
               unsigned int seed, int force)
{
	int ret = 0;
	ret = _avl_warmup_helper((avl_t *)rbt, nr_nodes, max_key, seed, force);
	return ret;
}

char *rbt_name()
{
	return "avl-rcu-htm-internal";
}
