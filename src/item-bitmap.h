#ifndef HBITMAP_CHECK_H
#define HBITMAP_CHECK_H

#include "postgres.h"
#include "access/heapam.h"

#define GetBitmapIndex(bmap, page, item) \
	((page == 0) ? (item) : (bmap->pages[page-1] + item))

#define MAX(a,b) ((a > b) ? a : b)

/* bitmap format */
typedef enum
{
	BITMAP_BASE64,
	BITMAP_HEX,
	BITMAP_BINARY,
	BITMAP_NONE
}	BitmapFormat;

/* bitmap, used to cross-check heap and indexes */
typedef struct item_bitmap
{
	/* current number of tracked pages */
	BlockNumber	npages;

	Size		nbytes;		/* used bytes */
	Size		maxBytes;	/* allocated bytes */

	/*
	 * running sum of items on a page, e.g. [10, 20, 30] if
	 * there are three pages and there are 10 items on each
	 */
	int		   *pages;

	/* data of the bitmap (0/1 for each item) */
	char	   *data;
} item_bitmap;


/* Allocates new item bitmap, sized for n pages.
 *
 * - npages : number of pages of the relation (needs to be known in advance)
 *
 * Prepares space for at least one item on each page, rounded to multiples
 * of PALLOC_CHUNK (1kB by default). So for example a 100-page table needs
 * at least 100 bits, so 1kB will be allocated, but a table with 100.000
 * pages (~800MB) will allocate 13kB initially.
 *
 * Returns the allocated bitmap.
 */
item_bitmap * bitmap_init(BlockNumber npages);

/* Copies the item bitmap (except the actual bitmap data, keeps zeroes).
 *
 * This is used to prepare a bitmap for index, matching the heap bitmap.
 *
 * Returns the new bitmap. */
item_bitmap * bitmap_copy(item_bitmap* src); /* preallocate empty bitmap */

/* Releases the bitmap, including the inner resources (allocated memory). */
void bitmap_free(item_bitmap* bitmap);

/* Resets the bitmap data (not the page counts) so that it can be reused
 * for another index on a given heap relation. */
void bitmap_reset(item_bitmap* bitmap);

/* Prepares the bitmap to accept data from another page - this only sets
 * the running item counts etc. and extends the internal structures.
 *
 * - bitmap : bitmap to update
 * - page : the next page to update (0, 1, 2, ... , npages-1)
 * - items : number of items on the page
 *
 * This needs to be called for a sequence of pages, starting with 0 and
 * increased by 1. Adding pages randomly will produce invalid bitmap.
 */
void bitmap_add_page(item_bitmap * bitmap, BlockNumber page, int nitems);

/* Updates the bitmap with all items from the heap page.
 *
 * - bitmap : bitmap to update
 * - header : page header
 * - raw_page : raw page data
 * - page : number of the page (0, 1, 2, ...)
 *
 * Returns number of issues (already set items).
 */
int bitmap_add_heap_items(item_bitmap * bitmap, PageHeader header,
						  char *raw_page, BlockNumber page);

/* Updates the bitmap with all items from the index (b-tree leaf) page.
 *
 * - bitmap : bitmap to update
 * - header : page header
 * - raw_page : raw page data
 * - page : number of the page (0, 1, 2, ...)
 *
 * Returns number of issues (already set items in the bitmap).
 */
int bitmap_add_index_items(item_bitmap * bitmap, PageHeader header,
						   char *raw_page, BlockNumber page);

/* Updates the bitmap so that the item (page,item) is either 0 or 1,
 * depending on the 'state' value (true => 1, false => 0).
 *
 * - bitmap : bitmap to update
 * - page : page number (where the item is, between 0 ...npages-1)
 * - item : position of the item on the page (0 .. max items)
 * - state : set or unset the item
 *
 * Returns false when the (page,item) is out of acceptable range or
 * when the bit is already set to the new value. Otherwise the
 * method returns true.
 */
bool bitmap_set_item(item_bitmap * bitmap, BlockNumber page,
					 int item, bool state);

/* Returns current bit value for the item (page,item).
 *
 * - bitmap : bitmap to update
 * - page : page number (where the item is, between 0 ...npages-1)
 * - item : position of the item on the page (0 .. max items)
 *
 * Returns false when the (page,item) is out of acceptable range.
 * Otherwise the bit value is returned.
 *
 * FIXME It's impossible to distinguish error and bit set to 0.
 */
bool bitmap_get_item(item_bitmap * bitmap, BlockNumber page, int item);

/* Counts the bits set to 1 in the bitmap
 *
 * - bitmap : bitmap to count
 *
 * Returns number of bits set to 1.
 */
uint64 bitmap_count(item_bitmap * bitmap);

/* Compares two bitmaps, returns number of differences.
 *
 * - bitmap_a : input bitmap
 * - bitmap_b : input bitmap
 *
 * Returns number of differences, i.e. bits set to 0 in bitmap_a
 * and 1 in bitmap_b, or vice versa.
 */
uint64 bitmap_compare(item_bitmap * bitmap_a, item_bitmap * bitmap_b);

/* Prints the info about the bitmap and the data as a series of 0/1. */
void bitmap_print(item_bitmap * bitmap, BitmapFormat format);

#endif   /* HEAP_CHECK_H */
