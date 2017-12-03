#ifndef HEAP_CHECK_H
#define HEAP_CHECK_H

#include "postgres.h"
#include "access/heapam.h"

uint32		check_heap_tuples(Relation rel, PageHeader header, char *buffer, BlockNumber block);

#endif							/* HEAP_CHECK_H */
