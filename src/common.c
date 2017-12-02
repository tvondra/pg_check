#include "common.h"

/*
 * check_page_header
 *		Perform global generic page checks (mostly info from the PageHeader).
 *
 * These apply to pages in all types of relations - tables, indexes, ...
 *
 * FIXME Check all the values for a page.
 *
 *	pd_lsn		- identifies xlog record for last change to this page.
 *	pd_tli		- ditto. (up to 9.2)
 *	pd_checksum - page checksum, if set (since 9.3)
 *	pd_flags	- flag bits.
 *	pd_lower	- offset to start of free space.
 *	pd_upper	- offset to end of free space.
 *	pd_special	- offset to start of special space.
 *	pd_pagesize_version - size in bytes and page layout version number.
 *	pd_prune_xid - oldest XID among potentially prunable tuples on page.
 *
 * FIXME Check size of the page (should be equal to BLCKSZ, see
 * PageGetPageSize and PageSizeIsValid macros (bufpage.h))
 *
 * FIXME Full page (lower == upper) should have PD_PAGE_FULL in pd_flags.
 */
uint32
check_page_header(PageHeader header, BlockNumber block)
{
	uint32 nerrs = 0;

	ereport(DEBUG1,
			(errmsg("[%d] header [lower=%d, upper=%d, special=%d free=%d]",
					block, header->pd_lower, header->pd_upper,
					header->pd_special, header->pd_upper - header->pd_lower)));

	/* check the page size (should be BLCKSZ) */
	if (PageGetPageSize(header) != BLCKSZ)
	{
		ereport(WARNING,
				(errmsg("[%d] invalid page size %d (%d)", block,
						(int) PageGetPageSize(header), BLCKSZ)));
		++nerrs;
	}

	/*
	 * This checks that the layout version is between 0 and 4, which are
	 * page versions supported by PostgreSQL. But the following checks
	 * depend on the format, so we only do them for the current version,
	 * which is PG_PAGE_LAYOUT_VERSION (4).
	 */
	if ((PageGetPageLayoutVersion(header) < 0) ||
		(PageGetPageLayoutVersion(header) > 4))
	{
		ereport(WARNING,
				(errmsg("[%d] invalid page layout version %d",
						block, PageGetPageLayoutVersion(header))));
		++nerrs;
	}
	else if (PageGetPageLayoutVersion(header) != 4)
	{
		/* obsolete page version, so no further checks */
		ereport(WARNING,
				(errmsg("[%d] invalid page layout version %d",
						block, PageGetPageLayoutVersion(header))));
		/*
		 * Increment the counter, to inform caller that this page does not
		 * have the expected format.
		 */
		return ++nerrs;
	}

	/*
	 * check that a page is new, and don't do any further checks for
	 * such pages
	 */
	if (PageIsNew(header))
	{
		/* obsolete page version, so no further checks */
		ereport(WARNING,
				(errmsg("[%d] page is new (pd_upper=0)", block)));
		/*
		 * We intentionally do not increment the counter here, as new pages
		 * are valid and expected to be found in relations. The rest of the
		 * code should expect this to and deal with it sanely.
		 *
		 * XXX Maybe we need to invent a different way to pass info that
		 * it's safe to perform further checks on the page?
		 */
		return nerrs;
	}

	/*
	 * All the pointers should be positive (greater than PageHeaderData)
	 * and less than BLCKSZ.
	 */
	if ((header->pd_lower < offsetof(PageHeaderData, pd_linp)) ||
		(header->pd_lower > BLCKSZ))
	{
		ereport(WARNING,
				(errmsg("[%d] lower %d not between %d and %d", block,
						header->pd_lower,
						(int) offsetof(PageHeaderData, pd_linp), BLCKSZ)));
		++nerrs;
	}

	if ((header->pd_upper < offsetof(PageHeaderData, pd_linp)) ||
		(header->pd_upper > BLCKSZ))
	{
		ereport(WARNING,
				(errmsg("[%d] upper %d not between %d and %d", block,
						header->pd_upper,
						(int) offsetof(PageHeaderData, pd_linp), BLCKSZ)));
		++nerrs;
	}

	if ((header->pd_special < offsetof(PageHeaderData, pd_linp)) ||
		(header->pd_special > BLCKSZ))
	{
		ereport(WARNING,
				(errmsg("[%d] special %d not between %d and %d", block,
						header->pd_special,
						(int) offsetof(PageHeaderData, pd_linp), BLCKSZ)));
		++nerrs;
	}

	/* upper should be >= lower */
	if (header->pd_lower > header->pd_upper)
	{
		ereport(WARNING,
				(errmsg("[%d] lower > upper (%d > %d)",
						block, header->pd_lower, header->pd_upper)));
		++nerrs;
	}

	/* special should be >= upper */
	if (header->pd_upper > header->pd_special)
	{
		ereport(WARNING,
				(errmsg("[%d] upper > special (%d > %d)",
						block, header->pd_upper, header->pd_special)));
		++nerrs;
	}

	/*
	 * PostgreSQL up to 9.2 had pd_tli in the header, with page timeline.
	 * 9.3 replaced that with a checksum, so be careful when reading and
	 * interpreting that value.
	 *
	 * We only check the timeline here, because we only deal with page
	 * headers here. Checksums are verified elsewhere.
	 */
#if (PG_VERSION_NUM < 90300)
	/* The timeline must not be greater than the current one. */
	if (header->pd_tli > ThisTimeLineID)
	{
		ereport(WARNING,
				(errmsg("[%d] invalid timeline %u (current %u)",
						block, header->pd_tli, ThisTimeLineID)));
		++nerrs;
	}
#endif

	/*
	 * Check that we only have valid flags set.
	 */
	if ((header->pd_flags & PD_VALID_FLAG_BITS) != header->pd_flags)
	{
		ereport(WARNING,
				(errmsg("[%d] page has invalid flags set %u",
						block, header->pd_flags)));
		++nerrs;
	}

	return nerrs;  
}
