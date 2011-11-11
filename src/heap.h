#ifndef HEAP_CHECK_H
#define HEAP_CHECK_H

#include "postgres.h"
#include "access/heapam.h"

uint32 check_heap_tuples(Relation rel, PageHeader header, char *buffer, int block);
uint32 check_heap_tuple(Relation rel, PageHeader header, int block, int i, char *buffer);
uint32 check_heap_tuple_attributes(Relation rel, PageHeader header, int block, int i, char *buffer);

#endif   /* HEAP_CHECK_H */