#include "common.h"

/*
FIXME Check all the values for a page.

 *		pd_lsn		- identifies xlog record for last change to this page.
 *		pd_tli		- ditto.
 *		pd_flags	- flag bits.
 *		pd_lower	- offset to start of free space.
 *		pd_upper	- offset to end of free space.
 *		pd_special	- offset to start of special space.
 *		pd_pagesize_version - size in bytes and page layout version number.
 *		pd_prune_xid - oldest XID among potentially prunable tuples on page.
*/

/* FIXME Check size of the page (should be equal to BLCKSZ, see PageGetPageSize and PageSizeIsValid macros (bufpage.h)) */
/* FIXME Full page (lower == upper) should have PD_PAGE_FULL in pd_flags. */
/* global checks (mostly info from the PageHeader) */
uint32 check_page_header(PageHeader header, int block) {
	
	uint32 nerrs = 0;
	
	ereport(DEBUG1, (errmsg("[%d] header [lower=%d, upper=%d, special=%d free=%d]", block, header->pd_lower, header->pd_upper, header->pd_special, header->pd_upper - header->pd_lower)));
	
	/* check the page size (should be BLCKSZ) */
	if (PageGetPageSize(header) != BLCKSZ) {
		ereport(WARNING,(errmsg("[%d] invalid page size %d (%d)", block, PageGetPageSize(header), BLCKSZ)));
		++nerrs;
	}
	
	/* FIXME This checks that the layout version is between 0 and 4, but the following checks 
	   depend on the format, so this should compare to PG_PAGE_LAYOUT_VERSION and continue only
	   if it's equal */
	if ((PageGetPageLayoutVersion(header) < 0) || (PageGetPageLayoutVersion(header) > 4)) {
		ereport(WARNING,(errmsg("[%d] invalid page layout version %d", block, PageGetPageLayoutVersion(header))));
		++nerrs;
	}
	
	/* FIXME a check that page is new (PageIsNew) might be appropriate here */

	/* all the pointers should be positive (greater than PageHeaderData) and less than BLCKSZ */
	if ((header->pd_lower < offsetof(PageHeaderData, pd_linp)) ||  (header->pd_lower > BLCKSZ)) {
		ereport(WARNING,(errmsg("[%d] lower %d not between %d and %d", block, header->pd_lower, offsetof(PageHeaderData, pd_linp), BLCKSZ)));
		++nerrs;
	}
	
	if ((header->pd_upper < offsetof(PageHeaderData, pd_linp)) ||  (header->pd_upper > BLCKSZ)) {
		ereport(WARNING,(errmsg("[%d] upper %d not between %d and %d", block, header->pd_upper, offsetof(PageHeaderData, pd_linp), BLCKSZ)));
		++nerrs;
	}
	
	if ((header->pd_special < offsetof(PageHeaderData, pd_linp)) ||  (header->pd_special > BLCKSZ)) {
		ereport(WARNING,(errmsg("[%d] special %d not between %d and %d", block, header->pd_special, offsetof(PageHeaderData, pd_linp), BLCKSZ)));
		++nerrs;
	}
	
	/* upper should be >= lower */
	if (header->pd_lower > header->pd_upper) {
		ereport(WARNING,(errmsg("[%d] lower > upper (%d > %d)", block, header->pd_lower, header->pd_upper)));
		++nerrs;
	}

	/* special should be >= upper */
	if (header->pd_upper > header->pd_special) {
		ereport(WARNING,(errmsg("[%d] upper > special (%d > %d)", block, header->pd_upper, header->pd_special)));
		++nerrs;
	}
	
	return nerrs;
  
}
