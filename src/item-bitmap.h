#ifndef HBITMAP_CHECK_H
#define HBITMAP_CHECK_H

#include "postgres.h"
#include "access/heapam.h"

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

	/*
	 * running sum of items on a page, e.g. [10, 20, 30] if
	 * there are three pages and there are 10 items on each
	 */
	uint64	   *pages;

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
