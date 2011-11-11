#ifndef INDEX_CHECK_H
#define INDEX_CHECK_H

#include "postgres.h"
#include "access/heapam.h"
#include "heap.h"

/* btree index checks */
uint32 check_index_page(Relation rel, PageHeader header, char *buffer, int block);
uint32 check_index_tuples(Relation rel, PageHeader header, char *buffer, int block);
uint32 check_index_tuple(Relation rel, PageHeader header, int block, int i, char *buffer);
uint32 check_index_tuple_attributes(Relation rel, PageHeader header, int block, int i, char *buffer);

#endif