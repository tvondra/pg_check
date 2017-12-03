#include "postgres.h"

#include "access/itup.h"
#include "access/nbtree.h"
#include "funcapi.h"
#include "utils/rel.h"

#include "common.h"
#include "index.h"
#include "item-bitmap.h"

#if (PG_VERSION_NUM >= 90600)
#include "catalog/pg_am.h"
#endif


#define BlockNum(tuple) \
(((tuple->t_tid.ip_blkid).bi_hi << 16) | ((uint16) (tuple->t_tid.ip_blkid).bi_lo))

/* generic check */
static uint32 generic_check_page(Relation rel, PageHeader header,
				   BlockNumber block, char *raw_page,
				   item_bitmap * bitmap);

/* btree checks */
static uint32 btree_check_page(Relation rel, PageHeader header,
				 BlockNumber block, char *raw_page,
				 item_bitmap * bitmap);
static uint32 btree_check_tuples(Relation rel, PageHeader header,
				   BlockNumber block, char *raw_page);
static uint32 btree_check_tuple(Relation rel, PageHeader header,
				  BlockNumber block, int i, char *raw_page);
static uint32 btree_check_attributes(Relation rel, PageHeader header,
					   BlockNumber block, OffsetNumber offnum,
					   char *raw_page, int dlen);
static uint32 btree_add_tuples(Relation rel, PageHeader header,
				 BlockNumber block, char *raw_page,
				 item_bitmap * bitmap);

struct index_check_methods
{
	Oid			oid;
	check_page_cb check_page;
	bool		crosscheck;
};

static struct index_check_methods methods[] = {
	{
		BTREE_AM_OID,
		btree_check_page,
		true
	},
	{
		InvalidOid,
		NULL,
		false
	}
};

check_page_cb
lookup_check_method(Oid oid, bool *crosscheck)
{
	int			i;

	*crosscheck = false;

	i = 0;
	while (methods[i].oid != InvalidOid)
	{
		if (methods[i].oid == oid)
		{
			if (crosscheck)
				*crosscheck = methods[i].crosscheck;

			return methods[i].check_page;
		}
	}

	return generic_check_page;
}

/*
 * FIXME Check that the index is consistent with the table - target
 * (block/item), etc.
 *
 * FIXME Check that there are no index items pointing to the same heap
 * tuple.
 *
 * FIXME Check number of valid items in an index (should be the same
 * as in the relation).
 *
 * FIXME Check basic XID assumptions (xmax >= xmin, ...).
 *
 * FIXME Check that there are no duplicate tuples in the index and that
 * all the table tuples are referenced (need to count tuples).
 *
 * FIXME This does not check that the tree structure is valid, just
 * individual pages. This might check that there are no cycles in the
 * index, that all the pages are actually used in the tree.
 *
 * FIXME Does not check (tid) referenced in the leaf-nodes, in the data
 * section.
 */

uint32
generic_check_page(Relation rel, PageHeader header, BlockNumber block,
				   char *raw_page, item_bitmap * bitmap)
{
	/* check basic page header */
	return check_page_header(header, block);
}

uint32
btree_check_page(Relation rel, PageHeader header, BlockNumber block,
				 char *raw_page, item_bitmap * bitmap)
{
	uint32		nerrs = 0;
	BTPageOpaque opaque = NULL;

	/* make sure we only ever call this for b-tree indexes */
	Assert(rel->rd_rel->relam == BTREE_AM_OID);

	/* check basic page header */
	nerrs += check_page_header(header, block);

	/* (block==0) means it's a meta-page, otherwise it's a regular index-page */
	if (block == BTREE_METAPAGE)
	{
		BTMetaPageData *mpdata = BTPageGetMeta(raw_page);

		ereport(DEBUG2,
				(errmsg("[%d] is a meta-page [magic=%d, version=%d]",
						block, mpdata->btm_magic, mpdata->btm_version)));

		if (mpdata->btm_magic != BTREE_MAGIC)
		{
			ereport(WARNING,
					(errmsg("[%d] metapage contains invalid magic number %d (should be %d)",
							block, mpdata->btm_magic, BTREE_MAGIC)));
			nerrs++;
		}

		if (mpdata->btm_version != BTREE_VERSION)
		{
			ereport(WARNING,
					(errmsg("[%d] metapage contains invalid version %d (should be %d)",
							block, mpdata->btm_version, BTREE_VERSION)));
			nerrs++;
		}

		/*
		 * FIXME Check that the btm_root/btm_fastroot is between 1 and number
		 * of index blocks
		 */

		/*
		 * FIXME Check that the btm_level/btm_fastlevel is equal to the level
		 * fo the root block
		 */

		return nerrs;
	}

	/* non-metapage */
	opaque = (BTPageOpaque) (raw_page + header->pd_special);

	/* check there's enough space for index-relevant data */
	if (header->pd_special > BLCKSZ - sizeof(BTPageOpaque))
	{
		ereport(WARNING,
				(errmsg("[%d] there's not enough special space for index data (%d > %d)",
						block,
						(int) sizeof(BTPageOpaque),
						BLCKSZ - header->pd_special)));
		nerrs++;
	}

	/*
	 * if the page is a leaf page, then level needs to be 0. Otherwise, it
	 * should be > 0. Deleted pages don't have a level, the level field is
	 * interleaved with an xid.
	 */
	if (!P_ISDELETED(opaque))
	{
		if (P_ISLEAF(opaque))
		{
			if (opaque->btpo.level != 0)
			{
				ereport(WARNING,
						(errmsg("[%d] is leaf page, but level %d is not zero",
								block, opaque->btpo.level)));
				nerrs++;
			}
		}
		else
		{
			if (opaque->btpo.level == 0)
			{
				ereport(WARNING,
						(errmsg("[%d] is a non-leaf page, but level is zero",
								block)));
				nerrs++;
			}
		}
	}

	/*
	 * XXX It probably does not make sense to try to cross-check tuples if the
	 * page header is corrupted. So check what check_index_page returns, and
	 * only proceed if there are no errors detected.
	 */
	nerrs += btree_check_tuples(rel, header, block, raw_page);

	/*
	 * If this is a leaf page (containing actual pointers to the heap), then
	 * update the bitmap.
	 */
	if (bitmap && P_ISLEAF(opaque))
		nerrs += btree_add_tuples(rel, header, block, raw_page, bitmap);

	return nerrs;
}

/* checks index tuples on the page, one by one */
uint32
btree_check_tuples(Relation rel, PageHeader header, BlockNumber block, char *raw_page)
{
	/* tuple checks */
	int			ntuples = PageGetMaxOffsetNumber(raw_page);
	int			i;
	uint32		nerrs = 0;

	ereport(DEBUG1,
			(errmsg("[%d] max number of tuples = %d", block, ntuples)));

	/*
	 * FIXME check btpo_flags (BTP_LEAF, BTP_ROOT, BTP_DELETED, BTP_META,
	 * BTP_HALF_DEAD, BTP_SPLIT_END and BTP_HAS_GARBAGE) and act accordingly.
	 */

	/* FIXME this should check lp_flags, just as the heap check */
	for (i = 0; i < ntuples; i++)
		nerrs += btree_check_tuple(rel, header, block, i, raw_page);

	if (nerrs > 0)
		ereport(WARNING,
				(errmsg("[%d] is probably corrupted, there were %d errors reported",
						block, nerrs)));

	return nerrs;
}

/* checks that the tuples do not overlap and then the individual attributes */
/* FIXME This should do exactly the same checks of lp_flags as in heap.c */
uint32
btree_check_tuple(Relation rel, PageHeader header, BlockNumber block,
				  int i, char *raw_page)
{
	int			dlen;
	uint32		nerrs = 0;
	int			j,
				a,
				b,
				c,
				d;

	ItemId		lp = &header->pd_linp[i];
	IndexTuple	itup;

	/* we can ignore unused items */
	if (lp->lp_flags == LP_UNUSED)
	{
		ereport(DEBUG2,
				(errmsg("[%d:%d] index item is unused",
						block, (i + 1))));
		return nerrs;
	}

	/*
	 * We only expect LP_NORMAL and LP_UNUSED items in indexes, so report any
	 * items with unexpected status.
	 */
	if (lp->lp_flags != LP_NORMAL)
	{
		ereport(DEBUG2,
				(errmsg("[%d:%d] index item has unexpected lp_flags (%u)",
						block, (i + 1), lp->lp_flags)));
		return ++nerrs;
	}

	/* OK, so this is LP_NORMAL index item, and we can inspect it. */

	itup = (IndexTuple) (raw_page + lp->lp_off);

	ereport(DEBUG2,
			(errmsg("[%d:%d] off=%d len=%d tid=(%d,%d)", block, (i + 1),
					lp->lp_off, lp->lp_len,
					ItemPointerGetBlockNumber(&(itup->t_tid)),
					ItemPointerGetOffsetNumber(&(itup->t_tid)))));

	/* check intersection with other tuples */

	/* [A,B] vs [C,D] */
	a = lp->lp_off;
	b = lp->lp_off + lp->lp_len;

	ereport(DEBUG2,
			(errmsg("[%d:%d] checking intersection with other tuples",
					block, (i + 1))));

	for (j = 0; j < i; j++)
	{
		ItemId		lp2 = &header->pd_linp[j];

		/*
		 * We only expect LP_NORMAL and LP_UNUSED items in (btree) indexes,
		 * and we can skip the unused ones.
		 */
		if (lp2->lp_flags == LP_UNUSED)
		{
			ereport(DEBUG3,
					(errmsg("[%d:%d] skipped (LP_UNUSED)", block, (j + 1))));
			continue;
		}
		else if (lp2->lp_flags != LP_NORMAL)
		{
			ereport(WARNING,
					(errmsg("[%d:%d] index item with unexpected flags (%d)",
							block, (j + 1), lp2->lp_flags)));
			continue;
		}

		c = lp2->lp_off;
		d = lp2->lp_off + lp2->lp_len;

		/* [A,C,B] or [A,D,B] or [C,A,D] or [C,B,D] */
		if (((a < c) && (c < b)) || ((a < d) && (d < b)) ||
			((c < a) && (a < d)) || ((c < b) && (b < d)))
		{
			ereport(WARNING,
					(errmsg("[%d:%d] intersects with [%d:%d] (%d,%d) vs. (%d,%d)",
							block, (i + 1), block, j, a, b, c, d)));
			++nerrs;
		}
	}

	/* compute size of the data stored in the index tuple */
	dlen = IndexTupleSize(itup) - IndexInfoFindDataOffset(itup->t_info);

	/* check attributes only for tuples with (lp_flags==LP_NORMAL) */
	nerrs += btree_check_attributes(rel, header, block, i + 1,
									raw_page, dlen);

	return nerrs;
}

/* checks the individual attributes of the tuple */
static uint32
btree_check_attributes(Relation rel, PageHeader header, BlockNumber block,
					   OffsetNumber offnum, char *raw_page, int dlen)
{
	IndexTuple	tuple;
	uint32		nerrs = 0;
	int			j,
				off;

	bits8	   *bitmap;
	BTPageOpaque opaque;
	ItemId		linp;
	bool		has_nulls = false;

	ereport(DEBUG2,
			(errmsg("[%d:%d] checking attributes for the tuple", block, offnum)));

	/* get the index tuple and info about the page */
	linp = &header->pd_linp[offnum - 1];
	tuple = (IndexTuple) (raw_page + linp->lp_off);
	opaque = (BTPageOpaque) (raw_page + header->pd_special);

	/* current attribute offset - always starts at (raw_page + off) */
	off = linp->lp_off + IndexInfoFindDataOffset(tuple->t_info);

	ereport(DEBUG3,
			(errmsg("[%d:%d] tuple has %d attributes", block, offnum,
					RelationGetNumberOfAttributes(rel))));

	/* XXX: MAXALIGN */
	bitmap = (bits8 *) (raw_page + linp->lp_off + sizeof(IndexTupleData));

	/*
	 * For non-leaf pages, the first data tuple may or may not actually have
	 * any data. See src/backend/access/nbtree/README, "Notes About Data
	 * Representation".
	 */
	if (!P_ISLEAF(opaque) && offnum == P_FIRSTDATAKEY(opaque) && dlen == 0)
	{
		ereport(DEBUG3,
				(errmsg("[%d:%d] first data key tuple on non-leaf block => no data, skipping",
						block, offnum)));
		return nerrs;
	}

	/*
	 * check all the index attributes
	 *
	 * TODO This is mostly copy'n'paste from check_heap_tuple_attributes, so
	 * maybe it could be refactored to share the code.
	 */
	for (j = 0; j < rel->rd_att->natts; j++)
	{
		Form_pg_attribute attr = rel->rd_att->attrs[j];

		/* actual length of the attribute value */
		int			len;

		/* copy from src/backend/commands/analyze.c */
		bool		is_varlena = (!attr->attbyval && attr->attlen == -1);
		bool		is_varwidth = (!attr->attbyval && attr->attlen < 0);

		/*
		 * if the attribute is marked as NULL (in the tuple header), skip to
		 * the next attribute
		 */
		if (IndexTupleHasNulls(tuple) && att_isnull(j, bitmap))
		{
			ereport(DEBUG3,
					(errmsg("[%d:%d] attribute '%s' is NULL (skipping)",
							block, offnum, attr->attname.data)));
			has_nulls = true;
			continue;
		}

		/* fix the alignment (see src/include/access/tupmacs.h) */
		off = att_align_pointer(off, attr->attalign, attr->attlen, raw_page + off);

		if (is_varlena)
		{
			/*
			 * We don't support toasted values in indexes, so this should not
			 * have the same issue as check_heap_tuple_attributes.
			 */

			len = VARSIZE_ANY(raw_page + off);

			if (len < 0)
			{
				ereport(WARNING,
						(errmsg("[%d:%d] attribute '%s' has negative length < 0 (%d)",
								block, offnum, attr->attname.data, len)));
				++nerrs;
				break;
			}

			if (VARATT_IS_COMPRESSED(raw_page + off))
			{
				/* the raw length should be less than 1G (and positive) */
				if ((VARRAWSIZE_4B_C(raw_page + off) < 0) ||
					(VARRAWSIZE_4B_C(raw_page + off) > 1024 * 1024))
				{
					ereport(WARNING,
							(errmsg("[%d:%d]  attribute '%s' has invalid length %d (should be between 0 and 1G)",
									block, offnum, attr->attname.data, VARRAWSIZE_4B_C(raw_page + off))));
					++nerrs;

					/*
					 * no break here, this does not break the page structure -
					 * we may check the other attributes
					 */
				}
			}

			/* FIXME Check if the varlena value may be detoasted. */

		}
		else if (is_varwidth)
		{
			/*
			 * get the C-string length (at most to the end of tuple), +1 as it
			 * does not include '\0' at the end
			 *
			 * if the string is not properly terminated, then this returns
			 * 'remaining space + 1' so it's detected
			 */
			len = strnlen(raw_page + off, linp->lp_off + len + linp->lp_len - off) + 1;
		}
		else
			/* attributes with fixed length */
			len = attr->attlen;

		Assert(len >= 0);

		/*
		 * Check if the length makes sense (is not negative and does not
		 * overflow the tuple end, stop validating the other rows (we don't
		 * know where to continue anyway).
		 */
		if ((dlen > 0) && (off + len > (linp->lp_off + linp->lp_len)))
		{
			ereport(WARNING,
					(errmsg("[%d:%d] attribute '%s' (off=%d len=%d) overflows tuple end (off=%d, len=%d)",
							block, offnum, attr->attname.data,
							off, len, linp->lp_off, linp->lp_len)));
			++nerrs;
			break;
		}

		/* skip to the next attribute */
		off += (dlen > 0) ? len : 0;

		ereport(DEBUG3,
				(errmsg("[%d:%d] attribute '%s' len=%d",
						block, offnum, attr->attname.data, len)));
	}

	ereport(DEBUG3,
			(errmsg("[%d:%d] last attribute ends at %d, tuple ends at %d",
					block, offnum, off, linp->lp_off + linp->lp_len)));

	/*
	 * Check if tuples with nulls (INDEX_NULL_MASK) actually have NULLs.
	 */
	if (IndexTupleHasNulls(tuple) && !has_nulls)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] tuple has INDEX_NULL_MASKL flag but no NULLs",
						block, offnum)));
		++nerrs;
	}

	/*
	 * after the last attribute, the offset should be less than the end of the
	 * tuple
	 */
	if (MAXALIGN(off) > linp->lp_off + linp->lp_len)
	{
		ereport(WARNING,
				(errmsg("[%d:%d] the last attribute ends at %d but the tuple ends at %d",
						block, offnum, off, linp->lp_off + linp->lp_len)));
		++nerrs;
	}

	return nerrs;
}

/* checks index tuples on the page, one by one */
static uint32
btree_add_tuples(Relation rel, PageHeader header, BlockNumber block,
				 char *raw_page, item_bitmap * bitmap)
{
	/* tuple checks */
	int			nerrs = 0;
	int			ntuples = PageGetMaxOffsetNumber(raw_page);
	BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(raw_page);
	int			item;
	int			start;

	/* skip first item (high key), except for the right-most page */
	start = (P_RIGHTMOST(opaque)) ? 0 : 1;

	for (item = start; item < ntuples; item++)
	{
		IndexTuple	itup;
		BlockNumber block;
		OffsetNumber offset;
		ItemId		lp = &header->pd_linp[item];

		/* we only care about LP_NORMAL items, skip others */
		if (lp->lp_flags != LP_NORMAL)
			continue;

		itup = (IndexTuple) (raw_page + lp->lp_off);

		offset = ItemPointerGetOffsetNumber(&(itup->t_tid)) - 1;
		block = ItemPointerGetBlockNumber(&(itup->t_tid));

		/* we should not have two index items pointing to the same tuple */
		if (bitmap_get(bitmap, block, offset))
			nerrs++;
		else
			bitmap_set(bitmap, block, offset);
	}

	return nerrs;
}
