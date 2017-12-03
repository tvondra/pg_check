#ifndef INDEX_CHECK_H
#define INDEX_CHECK_H

#include "postgres.h"
#include "access/heapam.h"
#include "heap.h"
#include "item-bitmap.h"

typedef uint32 (* check_page_cb)(Relation, PageHeader, BlockNumber,
								 char *, item_bitmap *);

check_page_cb lookup_check_method(Oid oid, bool *crosscheck);

#endif
