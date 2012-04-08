#include "item-bitmap.h"

#include <unistd.h>
#include <assert.h>

#include "access/itup.h"

/* allocate the memory in 1kB chunks - this needs to be large enough
 * to hold items for one page (8k ~ 290 bits, ie 32k ~ 1200 bits, so
 * this needs to be at least 150B)
 */
#define PALLOC_CHUNK 1024

int count_digits(int values[], int n);
char * itoa(int value, char * str, int maxlen);

/* init the bitmap (allocate, set default values) */
item_bitmap * bitmap_init(int npages) {
	
	item_bitmap * bitmap;

	/* sanity check */
	assert(npages >= 0);
	
	bitmap = (item_bitmap*)palloc(sizeof(item_bitmap));
	
	memset(bitmap, 0, sizeof(item_bitmap));
	
	bitmap->npages = npages;
	bitmap->pages = (int*)palloc(sizeof(int)*npages);
	
	bitmap->nbytes = 0;
	bitmap->maxBytes = PALLOC_CHUNK * (npages / PALLOC_CHUNK / 8 + 1);
	bitmap->data = (char*)palloc(bitmap->maxBytes);
	
	memset(bitmap->data, 0, bitmap->maxBytes);
	
	return bitmap;
	
}

/* copy the bitmap (except the actual bitmap data, keep zeroes) */
item_bitmap * bitmap_copy(item_bitmap * src) {
	
	item_bitmap * bitmap;
	
	/* sanity check */
	assert(src != NULL);
	
	bitmap = (item_bitmap*)palloc(sizeof(item_bitmap));
	
	bitmap->npages = src->npages;
	
	bitmap->nbytes = src->nbytes;
	bitmap->maxBytes = src->maxBytes;

	bitmap->pages = (int*)palloc(sizeof(int)*src->npages);
	memcpy(bitmap->pages, src->pages, sizeof(int)*src->npages);
	
	bitmap->data = (char*)palloc(src->maxBytes);
	memset(bitmap->data, 0, src->maxBytes);
	
	return bitmap;
	
}

/* reset the bitmap data (not the page counts etc.) */
void bitmap_reset(item_bitmap* bitmap) {
	memset(bitmap->data, 0, bitmap->maxBytes);
}

/* free the allocated resources */
void bitmap_free(item_bitmap* bitmap) {
	
	assert(bitmap != NULL);
	
	pfree(bitmap->pages);
	pfree(bitmap->data);
	pfree(bitmap);
}

/* needs to be called for paged 0,1,2,3,...npages (not randomly) */
/* extends the bitmap to handle another page */
void bitmap_add_page(item_bitmap * bitmap, int page, int items) {

	/* sanity checks */
	assert(page >= 0);
	assert(page < bitmap->npages);
	assert(items >= 0);
	assert(items <= MaxHeapTuplesPerPage);

	bitmap->pages[page] = (page == 0) ? items : (items + bitmap->pages[page-1]);
	
	/* if needed more bytes than already allocated, extend the bitmap */
	bitmap->nbytes = ((bitmap->pages[page] + 7) / 8);
	if (bitmap->nbytes > bitmap->maxBytes) {
		int len = bitmap->maxBytes; /* keep so that we can zero the new chunk */
		bitmap->maxBytes += PALLOC_CHUNK;
		bitmap->data = (char*)repalloc(bitmap->data, bitmap->maxBytes);
		memset(bitmap->data + len, 0, PALLOC_CHUNK);
	}

}

/* update the bitmap bith all items from a page (tracks number of items) */
int bitmap_add_heap_items(item_bitmap * bitmap, PageHeader header, char *raw_page, int page) {

	/* tuple checks */
	int nerrs = 0;
	int ntuples = PageGetMaxOffsetNumber(raw_page);
	int item;
	
	bitmap_add_page(bitmap, page, ntuples);
	
	/* by default set all LP_REDIRECT / LP_NORMAL items to '1' (we'll remove the HOT chains in the second pass) */
	/* FIXME what if there is a HOT chain and then an index is created? */
	for (item = 0; item < ntuples; item++) {
		if ((header->pd_linp[item].lp_flags == LP_NORMAL) || (header->pd_linp[item].lp_flags == LP_REDIRECT)) {
			if (! bitmap_set_item(bitmap, page, item, true)) {
				nerrs++;
			}
		}
	}
	
	/* second pass - remove the HOT chains */
	/* FIXME this seems not to work (the checks fail after update) */
	for (item = 0; item < ntuples; item++) {
	
		if (header->pd_linp[item].lp_flags == LP_REDIRECT) {
			/* walk only if not walked this HOT chain yet (skip the first item in the chain) */
			if (bitmap_get_item(bitmap, page, header->pd_linp[item].lp_off-1)) {
				if (! bitmap_set_item(bitmap, page, header->pd_linp[item].lp_off-1, false)) {
					/* FIXME this is incorrect, IMHO - the chain might be longer and the items may be
					 * processed out of order */
					nerrs++;
				}
			}
		}
		
	}
	
	return nerrs;

}

/* checks index tuples on the page, one by one */
int bitmap_add_index_items(item_bitmap * bitmap, PageHeader header, char *raw_page, int page) {

	/* tuple checks */
	int nerrs = 0;
	int ntuples = PageGetMaxOffsetNumber(raw_page);
	int item;
	
	for (item = 0; item < ntuples; item++) {

		IndexTuple itup = (IndexTuple)(raw_page + header->pd_linp[item].lp_off);
		if (! bitmap_set_item(bitmap, BlockIdGetBlockNumber(&(itup->t_tid.ip_blkid)), (itup->t_tid.ip_posid-1), true)) {
			nerrs++;
		}
		
	}
	
	return nerrs;
  
}

/* mark the (page,item) as occupied */
bool bitmap_set_item(item_bitmap * bitmap, int page, int item, bool state) {

	int byteIdx = (GetBitmapIndex(bitmap, page, item)) / 8;
	int bitIdx  = (GetBitmapIndex(bitmap, page, item)) % 8;
	
	if (page >= bitmap->npages) {
		elog(WARNING, "invalid page %d (max page %d)", page, bitmap->npages-1);
		return false;
	}
	
	if (byteIdx > bitmap->nbytes) {
		elog(WARNING, "invalid byte %d (max byte %d)", byteIdx, bitmap->nbytes);
		return false;
	}
	
	if (item >= bitmap->pages[page]) {
		elog(WARNING, "item %d out of range, page has only %d items", item, bitmap->pages[page]);
		return false;
	}

	/* FIXME check whether the item is aleady set or not (and return false if it is) */
	
	if (state) {
		/* set the bit (OR) */
		bitmap->data[byteIdx] |= (1 << bitIdx);
	} else {
		/* remove the bit (XOR) */
		bitmap->data[byteIdx] &= ~(1 << bitIdx);
	}
	
	return true;

}

/* check if the (page,item) is occupied */
bool bitmap_get_item(item_bitmap * bitmap, int page, int item) {

	int byteIdx = (GetBitmapIndex(bitmap, page, item)) / 8;
	int bitIdx  = (GetBitmapIndex(bitmap, page, item)) % 8;
	
	if (page >= bitmap->npages) {
		elog(WARNING, "invalid page %d (max page %d)", page, bitmap->npages-1);
		return false;
	}
	
	if (byteIdx > bitmap->nbytes) {
		elog(WARNING, "invalid byte %d (max byte %d)", byteIdx, bitmap->nbytes);
		return false;
	}
	
	if (item >= bitmap->pages[page]) {
		elog(WARNING, "item %d out of range, page has only %d items", item, bitmap->pages[page]);
		return false;
	}

	return (bitmap->data[byteIdx] && (1 << bitIdx));

}

/* counts bits set to 1 in the bitmap */
long bitmap_count(item_bitmap * bitmap) {

	long i, j, items = 0;
	
	for (i = 0; i < bitmap->nbytes; i++) {
		for (j = 0; j < 8; j++) {
			if (bitmap->data[i] & (1 << j)) {
				items++;
			}
		}
	}
	
	return items;
	
}

/* compare bitmaps, returns number of differences */
long bitmap_compare(item_bitmap * bitmap_a, item_bitmap * bitmap_b) {

	long i, j, ndiff = 0;
	char diff = 0;

	/* compare number of pages and total items */
	/* FIXME this rather a sanity check, because these values are copied by bitmap_prealloc */
	if (bitmap_a->npages != bitmap_b->npages) {
		elog(WARNING, "bitmaps do not track the same number of pages (%d != %d)",
			 bitmap_a->npages, bitmap_b->npages);
		return MAX(bitmap_a->pages[bitmap_a->npages-1],
				   bitmap_b->pages[bitmap_b->npages-1]);
	} else if (bitmap_a->pages[bitmap_a->npages-1] != bitmap_b->pages[bitmap_b->npages-1]) {
		elog(WARNING, "bitmaps do not track the same number of pages (%d != %d)",
			 bitmap_a->pages[bitmap_a->npages-1], bitmap_b->pages[bitmap_b->npages-1]);
	}
	
	/* the actual check, compares the bits one by one */
	for (i = 0; i < bitmap_a->nbytes; i++) {
		diff = (bitmap_a->data[i] ^ bitmap_b->data[i]);
		if (diff != 0) {
			for (j = 0; j < 8; j++) {
				if (diff & (1 << j)) {
					ndiff++;
				}
			}
		}
	}
	
	return ndiff;
	
}

/* Prints the info about the bitmap and the data as a series of 0/1. */
/* TODO print details about differences (items missing in heap, items missing in index) */
void bitmap_print(item_bitmap * bitmap) {
	
	int i, j, bits = 0, k = 0;
	char data[bitmap->nbytes*8+1];
	int len = count_digits(bitmap->pages, bitmap->npages) + bitmap->npages;
	char pages[len];
	char *ptr = pages;
	
	ptr[0] = '\0';
	for (i = 0; i < bitmap->npages; i++) {
		ptr = itoa(bitmap->pages[i], ptr, len - (ptr - pages));
		*(ptr++) = ',';
	}
	*(--ptr) = '\0';
	
	for (i = 0; i < bitmap->nbytes; i++) {
		for (j = 0; j < 8; j++) {
			if (bitmap->data[i] & (1 << j)) {
				data[k++] = '1';
			} else {
				data[k++] = '0';
			}
		}
	}
	data[k] = '\0';
	
	bits = bitmap_count(bitmap);
	
	elog(WARNING, "bitmap nbytes=%d nbits=%d npages=%d pages=[%s] data=[%s]",
		 bitmap->nbytes, bits, bitmap->npages, pages, data);
	
}

/* count digits to print the array (in ASCII) */
int count_digits(int values[], int n) {
	int i, digits = 0;
	for (i = 0; i < n; i++) {
		digits += (int)ceil(log(values[i]) / log(10));
	}
	return digits;
}

/* utility to fill an integer value in a given value */
char * itoa(int value, char * str, int maxlen) {
	return str + snprintf(str, maxlen, "%d", value);
}
