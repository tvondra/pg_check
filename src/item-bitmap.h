#ifndef HBITMAP_CHECK_H
#define HBITMAP_CHECK_H

#include "postgres.h"
#include "access/heapam.h"

#define GetBitmapIndex(bmap, page, item) \
	((page == 0) ? (item) : (bmap->pages[page-1] + item))

#define MAX(a,b) ((a > b) ? a : b)

/* bitmap, used to cross-check heap and indexes */
typedef struct item_bitmap {
	
	/* current number of tracked pages */
	int npages;
	
	int nbytes;		/* used bytes */
	int maxBytes;	/* allocated bytes */
	
	/* running sum of items on a page, e.g. [10, 20, 30] if
	   there are three pages and there are 10 items on each */
	int * pages;
	
	/* data of the bitmap (0/1 for each item) */
	char * data;
	
} item_bitmap;


/* allocate the bitmap (~4.5MB for each 1GB of heap) */
item_bitmap * bitmap_alloc(int npages);
item_bitmap * bitmap_prealloc(item_bitmap* src); /* preallocate empty bitmap */
void bitmap_free(item_bitmap* bitmap);

void bitmap_reset(item_bitmap* bitmap);

/* extends the bitmap to handle another page */
void bitmap_add_page(item_bitmap * bitmap, int page, int items);

/* update the bitmap with all items from the table */
int bitmap_add_heap_items(item_bitmap * bitmap, PageHeader header, char *raw_page, int page);
int bitmap_add_index_items(item_bitmap * bitmap, PageHeader header, char *raw_page, int page);

/* mark the (page,item) as occupied */
bool bitmap_set_item(item_bitmap * bitmap, int page, int item, bool state);

/* check if the (page,item) is occupied */
bool bitmap_get_item(item_bitmap * bitmap, int page, int item);

/* count bits set in the bitmap */
long bitmap_count(item_bitmap * bitmap);

/* compare bitmaps, returns number of differences */
long bitmap_compare(item_bitmap * bitmap_a, item_bitmap * bitmap_b);

void bitmap_print(item_bitmap * bitmap);

#endif   /* HEAP_CHECK_H */