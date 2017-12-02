#include "postgres.h"

#include "access/htup.h"
#if (PG_VERSION_NUM >= 90300)
#include "access/htup_details.h"
#endif
#include "funcapi.h"
#include "utils/rel.h"

#include "heap.h"


static uint32 check_heap_tuple(Relation rel, PageHeader header,
							   BlockNumber block, int i, char *buffer);

static uint32 check_heap_tuple_attributes(Relation rel, PageHeader header,
							   BlockNumber block, int i, char *buffer);

/* checks heap tuples (table) on the page, one by one */
uint32
check_heap_tuples(Relation rel, PageHeader header, char *buffer,
				  BlockNumber block)
{
	/* tuple checks */
	int		ntuples = PageGetMaxOffsetNumber(buffer);
	int		i;
	uint32	nerrs = 0;

	ereport(DEBUG1,
			(errmsg("[%d] max number of tuples = %d", block, ntuples)));

	for (i = 0; i < ntuples; i++)
		nerrs += check_heap_tuple(rel, header, block, i, buffer);

	if (nerrs > 0)
		ereport(WARNING,
				(errmsg("[%d] is probably corrupted, there were %d errors reported",
						block, nerrs)));

	return nerrs;
}

/* checks that the tuples do not overlap and then the individual attributes */
static uint32
check_heap_tuple(Relation rel, PageHeader header, BlockNumber block,
				 int i, char *buffer)
{
	uint32	nerrs = 0;
	int		j, a, b, c, d;
	ItemId	lp;

	/* get pointer to the line pointer */
	lp = &header->pd_linp[i];

	/* check length with respect to lp_flags (unused, normal, redirect, dead) */
	if (lp->lp_flags == LP_REDIRECT)
	{
		ereport(DEBUG2,
				(errmsg("[%d:%d] tuple is LP_REDIRECT", block, (i+1))));

		/* FIXME check that the LP_REDIRECT target is OK (exists, not empty) to handle HOT tuples properly */
		/* items with LP_REDIRECT need to be handled differently (lp_off holds the link to the next tuple pointer) */
		if (lp->lp_len != 0)
		{
			ereport(WARNING,
					(errmsg("[%d:%d] tuple with LP_REDIRECT and len != 0 (%d)",
							block, (i+1), lp->lp_len)));
			++nerrs;
		}

		return nerrs;
	}
	else if (lp->lp_flags == LP_UNUSED)
	{
		ereport(DEBUG2,
				(errmsg("[%d:%d] tuple is LP_UNUSED", block, (i+1))));

		/* LP_UNUSED => (len = 0) */
		if (lp->lp_len != 0)
		{
			ereport(WARNING,
					(errmsg("[%d:%d] tuple with LP_UNUSED and len != 0 (%d)",
							block, (i+1), lp->lp_len)));
			++nerrs;
		}

		return nerrs;
	}
	else if (lp->lp_flags == LP_DEAD)
	{
		/*
		 * Dead tuples may or may not have storage, depending on if vacuum
		 * did the first part of heap cleanup. If there is no storage, we
		 * don't have anything to check. If there is storage, we do the
		 * same check as for LP_NORMAL.
		 */
		ereport(DEBUG2,
				(errmsg("[%d:%d] tuple is LP_DEAD", block, (i+1))));

		/*
		 * No storage, so we're done with this item pointer.
		 *
		 * XXX Maybe check that lp_off is set to 0 too?
		 */
		if (lp->lp_len == 0)
			return nerrs;
	}
	else if (lp->lp_flags == LP_NORMAL)
	{
		ereport(DEBUG2,
				(errmsg("[%d:%d] tuple is LP_NORMAL", block, (i+1))));
	}
	else
	{
		ereport(WARNING,
				(errmsg("[%d:%d] item has unknown lp_flag %u",
						block, (i+1), lp->lp_flags)));
		return ++nerrs;
	}

	/*
	 * So the item is either LP_NORMAL or LP_DEAD with storage. Check that
	 * the values (length and offset) are within reasonable boundaries
	 * (that is, between 0 and BLCKSZ).
	 *
	 * Note: The lp_len and lp_off fields are defined as unsigned, so it
	 * does not make sense to check for negative values. Equality is enough.
	 */
	if (lp->lp_len == 0)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] tuple with length = 0 (%d)",
						block, (i+1), lp->lp_len)));
		++nerrs;
	}

	if (lp->lp_off == 0)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] tuple with offset <= 0 (%d)",
						block, (i+1), lp->lp_off)));
		++nerrs;
	}

	/*
	 * Check both the starting and ending positions are the page (we have
	 * checked that pd_upper/pd_special are valid with respect to BLCKSZ
	 * in check_page_header).
	 */
	if (lp->lp_off < header->pd_upper)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] tuple with offset - length < upper (%d - %d < %d)",
						block, (i+1), lp->lp_off,
						lp->lp_len, header->pd_upper)));
		++nerrs;
	}

	if (lp->lp_off + lp->lp_len > header->pd_special)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] tuple with offset > special (%d > %d)",
						block, (i+1), lp->lp_off, header->pd_special)));
		++nerrs;
	}

	/*
	 * Check intersection with other tuples on the page. We only check
	 * preceding line pointers, as the subsequent will be cross-checked
	 * when check_heap_tuple is called for them.
	 */

	/* [A,B] vs [C,D] */
	a = lp->lp_off;
	b = lp->lp_off + lp->lp_len;

	for (j = 0; j < i; j++)
	{
		ItemId	lp2 = &header->pd_linp[j];

		/*
		 * We care about items with storage here, so we can skip LP_UNUSED
		 * and LP_REDIRECT right away, and LP_DEAD if they have no storage.
		 */
		if (lp2->lp_flags == LP_UNUSED ||
			lp2->lp_flags == LP_REDIRECT ||
			(lp2->lp_flags == LP_DEAD && lp2->lp_len == 0))
			continue;

		c = lp2->lp_off;
		d = lp2->lp_off + lp2->lp_len;

		/* [A,C,B] or [A,D,B] or [C,A,D] or [C,B,D] */
		if (((a < c) && (c < b)) || ((a < d) && (d < b)) ||
			((c < a) && (a < d)) || ((c < b) && (b < d)))
		{
			ereport(WARNING,
					(errmsg("[%d:%d] intersects with [%d:%d] (%d,%d) vs. (%d,%d)",
							block, (i+1), block, j, a, b, c, d)));
			++nerrs;
		}
	}

	return nerrs + check_heap_tuple_attributes(rel, header, block, i, buffer);
}

/* checks the individual attributes of the tuple */
static uint32
check_heap_tuple_attributes(Relation rel, PageHeader header, BlockNumber block,
							int i, char *buffer)
{
	HeapTupleHeader	tupheader;
	uint32			nerrs = 0;
	int				j, off, endoff;
	int				tuplenatts;
	bool			has_nulls = false;

	ItemId			lp = &header->pd_linp[i];

	ereport(DEBUG2,
			(errmsg("[%d:%d] checking attributes for the tuple", block, (i+1))));

	/*
	 * Get the header of the tuple (it starts at the 'lp_off' offset and
	 * it's t_hoff long (incl. bitmap)).
	 */
	tupheader = (HeapTupleHeader)(buffer + lp->lp_off);

	/* attribute offset - always starts at (buffer + off) */
	off = lp->lp_off + tupheader->t_hoff;

	tuplenatts = HeapTupleHeaderGetNatts(tupheader);

	/*
	 * It's possible that the tuple descriptor has more attributes than the
	 * on-disk tuple. That can happen e.g. after a new attribute is added
	 * to the table in a wat that does not require table rewrite.
	 *
	 * However, the opposite should not happen - the on-disk tuple must not
	 * have more attributes than the descriptor.
	 */
	if (tuplenatts > rel->rd_att->natts)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] tuple has too many attributes. %d found, %d expected",
						block, (i+1),
						HeapTupleHeaderGetNatts(tupheader),
						RelationGetNumberOfAttributes(rel))));
		return ++nerrs;
	}

	ereport(DEBUG3,
			(errmsg("[%d:%d] tuple has %d attributes (%d in relation)",
					block, (i+1), tuplenatts, rel->rd_att->natts)));

	/* check all the attributes */
	for (j = 0; j < tuplenatts; j++)
	{
		Form_pg_attribute	attr = rel->rd_att->attrs[j];

		/* actual length of the attribute value */
		int len;

		/* copied from src/backend/commands/analyze.c */
		bool is_varlena  = (!attr->attbyval && attr->attlen == -1);
		bool is_varwidth = (!attr->attbyval && attr->attlen < 0);

		/*
		 * If the attribute is marked as NULL (in the tuple header), skip
		 * to the next attribute. The bitmap is only present when the
		 * tuple has HEAP_HASNULL flag.
		 */
		if ((tupheader->t_infomask & HEAP_HASNULL) &&
			att_isnull(j, tupheader->t_bits))
		{
			ereport(DEBUG3,
					(errmsg("[%d:%d] attribute '%s' is NULL (skipping)",
							block, (i+1), attr->attname.data)));
			has_nulls = true; /* remember we've seen NULL value */
			continue;
		}

		/* fix the alignment */
		off = att_align_pointer(off, attr->attalign, attr->attlen, buffer+off);

		if (is_varlena)
		{
			/*
			 * FIXME This seems wrong, because VARSIZE_ANY will return length
			 * of the actual value, not the on-disk length. That may differ
			 * for TOASTed values, I guess.
			 */

			len = VARSIZE_ANY(buffer + off);

			if (len < 0)
			{
				ereport(WARNING,
						(errmsg("[%d:%d] attribute '%s' has negative length < 0 (%d)",
								block, (i+1), attr->attname.data, len)));
				++nerrs;
				break;
			}

			if (VARATT_IS_COMPRESSED(buffer + off))
			{
				/* the raw length should be less than 1G (and positive) */
				if ((VARRAWSIZE_4B_C(buffer + off) < 0) ||
					(VARRAWSIZE_4B_C(buffer + off) > 1024*1024))
				{
					ereport(WARNING,
							(errmsg("[%d:%d]  attribute '%s' has invalid length %d (should be between 0 and 1G)",
									block, (i+1), attr->attname.data, VARRAWSIZE_4B_C(buffer + off))));
					++nerrs;
					/* XXX maybe check ((toast_pointer).va_extsize < (toast_pointer).va_rawsize - VARHDRSZ) */
					/*
					 * No break here, as this does not break the page structure,
					 * so we may check the other attributes.
					 */
				}
			}

			/*
			 * FIXME  Check if the varlena value can be detoasted - see
			 * heap_tuple_untoast_attr in backend/access/heap/tuptoaster.c.
			 */
		}
		else if (is_varwidth)
		{
			/* get the C-string length (at most to the end of tuple), +1 as it does not include '\0' at the end */
			/* if the string is not properly terminated, then this returns 'remaining space + 1' so it's detected */
			len = strnlen(buffer + off, lp->lp_off + len + lp->lp_len - off) + 1;
		}
		else
			/* attributes with fixed length */
			len = attr->attlen;

		/*
		 * Check if the length makes sense (is not negative and does not overflow
		 * the tuple end, stop validating the other rows (we don't know where to
		 * continue anyway).
		 */
		endoff = lp->lp_off + lp->lp_len;
		if (off + len > endoff)
		{
			ereport(WARNING,
					(errmsg("[%d:%d] attribute '%s' (off=%d len=%d) overflows tuple end (off=%d, len=%d)",
							block, (i+1), attr->attname.data,
							off, len, lp->lp_off, lp->lp_len)));
			++nerrs;
			break;
		}

		Assert(len >= 0);

		/* skip to the next attribute */
		off += len;

		ereport(DEBUG3,
				(errmsg("[%d:%d] attribute '%s' length=%d",
						block, (i+1), attr->attname.data, len)));
	}

	ereport(DEBUG3,
			(errmsg("[%d:%d] last attribute ends at %d, tuple ends at %d",
					block, (i+1), off, lp->lp_off + lp->lp_len)));

	/*
	 * Check if tuples with HEAP_HASNULL actually have NULL attribute.
	 */
	if ((tupheader->t_infomask & HEAP_HASNULL) && !has_nulls)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] has HEAP_HASNULL flag but no NULLs",
						block, (i+1))));
		++nerrs;
	}

	/*
	 * The end of last attribute should fall within the length given in
	 * the line pointer.
	 */
	endoff = lp->lp_off + lp->lp_len;
	if (off > endoff)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] the last attribute ends at %d but the tuple ends at %d",
						block, (i+1), off, endoff)));
		++nerrs;
	}

	return nerrs;
}
