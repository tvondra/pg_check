#include "item-bitmap.h"

#include "access/itup.h"

#if (PG_VERSION_NUM >= 90300)
#include <math.h>
#include "access/nbtree.h"
#include "access/htup_details.h"
#endif

/*
 * The bitmap is allocated as one chunk of memory, assuming space for
 * MaxHeapTuplesPerPage items on each page. That means about 40B for
 * 8kB pages and 150B for 32kB pages.
 */
#define BITMAP_BYTES_PER_PAGE	((Size)(MaxHeapTuplesPerPage + 7) / 8)

#define GetBitmapIndex(p,o)	((p) * MaxHeapTuplesPerPage + (o))
#define GetBitmapByte(p,o)	(GetBitmapIndex(p,o) / 8)
#define GetBitmapBit(p,o)	(GetBitmapIndex(p,o) % 8)

static int count_digits(uint64 values[], BlockNumber n);
static char * itoa(int value, char * str, int maxlen);
static char * hex(const char * data, int n);
static char * binary(const char * data, int n);
static char * base64(const char * data, int n);

/* init the bitmap (allocate, set default values) */
item_bitmap *
bitmap_init(BlockNumber npages)
{
	item_bitmap * bitmap;

	/* sanity check */
	Assert(npages >= 0);

	bitmap = (item_bitmap *) palloc0(sizeof(item_bitmap));

	bitmap->npages = npages;
	bitmap->pages = (uint64 *) palloc0(sizeof(uint64) * npages);

	bitmap->nbytes = npages * BITMAP_BYTES_PER_PAGE;
	bitmap->data = (char *) palloc0(bitmap->nbytes);

	return bitmap;
}

/* copy the bitmap (except the actual bitmap data, keep zeroes) */
item_bitmap *
bitmap_copy(item_bitmap * src)
{
	item_bitmap * bitmap;

	/* sanity check */
	Assert(src != NULL);

	bitmap = (item_bitmap *) palloc0(sizeof(item_bitmap));

	bitmap->npages = src->npages;
	bitmap->nbytes = src->nbytes;

	bitmap->pages = (uint64 *) palloc(sizeof(uint64) * src->npages);
	memcpy(bitmap->pages, src->pages, sizeof(uint64) * src->npages);

	bitmap->data = (char *) palloc(src->nbytes);
	memset(bitmap->data, 0, src->nbytes);

	return bitmap;
}

/* reset the bitmap data (not the page counts etc.) */
void
bitmap_reset(item_bitmap* bitmap)
{
	memset(bitmap->data, 0, bitmap->nbytes);
}

/* free the allocated resources */
void
bitmap_free(item_bitmap* bitmap)
{
	Assert(bitmap != NULL);

	pfree(bitmap->pages);
	pfree(bitmap->data);
	pfree(bitmap);
}

/* update the bitmap with all items from a page (tracks number of items) */
int
bitmap_add_heap_items(item_bitmap * bitmap, PageHeader header,
					  char *raw_page, BlockNumber page)
{
	/* tuple checks */
	int		nerrs = 0;
	int		ntuples = PageGetMaxOffsetNumber(raw_page);
	int		item;
	Page	p = (Page)raw_page;
	bool	add[MaxHeapTuplesPerPage];

	/* assume we're adding all items from this heap page */
	memset(add, 1, sizeof(add));

	/*
	 * Walk and remove all LP_UNUSED pointers, and LP_REDIRECT targets.
	 *
	 * XXX Do we need to do something about LP_DEAD rows here? At this
	 * point we keep them in the bitmap.
	 */
	for (item = 0; item < ntuples; item++)
	{
		ItemId	lp = &header->pd_linp[item];

		if (lp->lp_flags == LP_UNUSED)
			add[item] = false;

		if (lp->lp_flags == LP_REDIRECT)
			add[lp->lp_off - 1] = false;
	}

	/*
	 * Deal with HOT chains. For every LP_NORMAL, and LP_DEAD pointer with
	 * storage (i.e. lp_len>0), see if the tuple is HOT-updated (i.e. if it
	 * has HEAP_HOT_UPDATED set). If yes, remove it from the bitmap.
	 */
	for (item = 0; item < ntuples; item++)
	{
		ItemId	lp = &header->pd_linp[item];

		if ((lp->lp_flags == LP_NORMAL) ||
			((lp->lp_flags == LP_DEAD) && (lp->lp_len > 0)))
		{
			HeapTupleHeader htup;

			htup = (HeapTupleHeader) PageGetItem(p, lp);

			if (HeapTupleHeaderIsHeapOnly(htup))
				add[item] = false;
		}
	}

	for (item = 0; item < ntuples; item++)
	{
		if (add[item])
		{
			/* increment number of items tracked on this page */
			bitmap->pages[page]++;
			bitmap_set(bitmap, page, item);
		}
	}

	return nerrs;
}

/* mark the (page,item) as occupied */
void
bitmap_set(item_bitmap *bitmap, BlockNumber page, int item)
{
	int byte = GetBitmapByte(page, item);
	int bit  = GetBitmapBit(page, item);

	if (page >= bitmap->npages)
	{
		elog(WARNING, "invalid page %d (max page %d)", page, bitmap->npages-1);
		return;
	}

	if (byte > bitmap->nbytes)
	{
		elog(WARNING, "invalid byte %d (max byte %zu)", byte, bitmap->nbytes);
		return;
	}

	/* set the bit (OR) */
	bitmap->data[byte] |= (0x01 << bit);
}

/* check if the (page,item) is occupied */
bool
bitmap_get(item_bitmap * bitmap, BlockNumber page, int item)
{
	int byte = GetBitmapByte(page, item);
	int bit  = GetBitmapBit(page, item);

	if (page >= bitmap->npages)
	{
		elog(WARNING, "invalid page %d (max page %d)", page, bitmap->npages-1);
		return false;
	}

	if (byte > bitmap->nbytes)
	{
		elog(WARNING, "invalid byte %d (max byte %zu)", byte, bitmap->nbytes);
		return false;
	}

	return (bitmap->data[byte] & (0x01 << bit));
}

/* counts bits set to 1 in the bitmap */
uint64
bitmap_count(item_bitmap * bitmap)
{
	Size i;
	int j;
	uint64 items = 0;

	for (i = 0; i < bitmap->nbytes; i++)
	{
		for (j = 0; j < 8; j++)
		{
			if (bitmap->data[i] & (1 << j))
				items++;
		}
	}

	return items;
}

/* compare bitmaps, returns number of differences */
uint64
bitmap_compare(item_bitmap * bitmap_a, item_bitmap * bitmap_b)
{
	BlockNumber		block;
	OffsetNumber	offset;
	uint64		ndiff;

	Assert(bitmap_a->nbytes == bitmap_b->nbytes);
	Assert(bitmap_a->npages == bitmap_b->npages);

	/* the actual check, compares the bits one by one */
	ndiff = 0;
	for (block = 0; block < bitmap_a->npages; block++)
	{
		for (offset = 0; offset < MaxHeapTuplesPerPage; offset++)
		{
			if (bitmap_get(bitmap_a, block, offset) != bitmap_get(bitmap_a, block, offset))
			{
				elog(WARNING, "bitmap mismatch of [%u,%d]", block, offset);
				ndiff++;
			}
		}
	}

	return 0;
}

/* Prints the info about the bitmap and the data as a series of 0/1. */
/* TODO print details about differences (items missing in heap, items missing in index) */
void
bitmap_print(item_bitmap * bitmap, BitmapFormat format)
{
	int i = 0;
	int len = count_digits(bitmap->pages, bitmap->npages) + bitmap->npages;
	char pages[len];
	char *ptr = pages;
	char *data = NULL;

	ptr[0] = '\0';
	for (i = 0; i < bitmap->npages; i++)
	{
		ptr = itoa(bitmap->pages[i], ptr, len - (ptr - pages));
		*(ptr++) = ',';
	}
	*(--ptr) = '\0';

	/* encode as binary or hex */
	if (format == BITMAP_BINARY)
		data = binary(bitmap->data, bitmap->nbytes);
	else if (format == BITMAP_BASE64)
		data = base64(bitmap->data, bitmap->nbytes);
	else if (format == BITMAP_HEX)
		data = hex(bitmap->data, bitmap->nbytes);
	else if (format == BITMAP_NONE)
	{
		data = palloc(1);
		data[0] = '\0';
	}

	if (format == BITMAP_NONE)
	{
		elog(WARNING, "bitmap nbytes=%zu nbits=%ld npages=%d pages=[%s]",
			bitmap->nbytes, bitmap_count(bitmap), bitmap->npages, pages);
	}
	else
	{
		elog(WARNING, "bitmap nbytes=%zu nbits=%ld npages=%d pages=[%s] data=[%s]",
			bitmap->nbytes, bitmap_count(bitmap), bitmap->npages, pages, data);
	}

	pfree(data);
}

/* count digits to print the array (in ASCII) */
static int
count_digits(uint64 values[], BlockNumber n)
{
	int i, digits = 0;
	for (i = 0; i < n; i++)
		digits += (int)ceil(log(values[i]) / log(10));

	return digits;
}

/* utility to fill an integer value in a given value */
static char *
itoa(int value, char * str, int maxlen)
{
	return str + snprintf(str, maxlen, "%d", value);
}

/* encode data to hex */
static char *
hex(const char * data, int n)
{
	int i, w = 0;
	static const char hex[] = "0123456789abcdef";
	char * result = palloc(n*2+1);

	for (i = 0; i < n; i++) {
		result[w++] = hex[(data[i] >> 4) & 0x0F];
		result[w++] = hex[data[i] & 0x0F];
	}

	result[w] = '\0';

	return result;
}

static char *
binary(const char * data, int n)
{
	int i, j, k = 0;
	char *result = palloc(n*8+10);

	for (i = 0; i < n; i++)
	{
		for (j = 0; j < 8; j++)
		{
			if (data[i] & (1 << j))
				result[k++] = '1';
			else
				result[k++] = '0';
		}
	}

	result[k] = '\0';

	return result;
}

/* encode data to base64 */
static char *
base64(const char * data, int n)
{
	int i, k = 0;
	static const char	_base64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	char 			   *result = palloc(4*((n+2)/3) + 1);
	uint32				buf = 0;
	int					pos = 2;

	for (i = 0; i < n; i++)
	{
		buf |= data[i] << (pos << 3);
		pos--;

		if (pos < 0)
		{
			result[k++] = _base64[(buf >> 18) & 0x3f];
			result[k++] = _base64[(buf >> 12) & 0x3f];
			result[k++] = _base64[(buf >> 6) & 0x3f];
			result[k++] = _base64[buf & 0x3f];

			pos = 2;
			buf = 0;
		}
	}

	if (pos != 2)
	{
		result[k++] = _base64[(buf >> 18) & 0x3f];
		result[k++] = _base64[(buf >> 12) & 0x3f];
		result[k++] = (pos == 0) ? _base64[(buf >> 6) & 0x3f] : '\0';
	}

	result[k] = '\0';

	return result;
}
