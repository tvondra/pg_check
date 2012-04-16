#include "index.h"
#include "common.h"

#include "postgres.h"

#include "access/itup.h"
#include "access/nbtree.h"
#include "funcapi.h"
#include "utils/rel.h"

#include "heap.h"

#define BlockNum(tuple) \
(((tuple->t_tid.ip_blkid).bi_hi << 16) | ((uint16) (tuple->t_tid.ip_blkid).bi_lo))

/* FIXME Check that the index is consistent with the table - target (block/item), etc. */
/* FIXME Check that there are no index items pointing to the same heap tuple. */
/* FIXME Check number of valid items in an index (should be the same as in the relation). */
/* FIXME Check basic XID assumptions (xmax >= xmin, ...). */
/* FIXME Check that there are no duplicate tuples in the index and that all the table tuples are referenced (need to count tuples). */
/* FIXME This does not check that the tree structure is valid, just individual pages. This might check that there are no cycles in the index, that all the pages are actually used in the tree. */
/* FIXME Does not check (tid) referenced in the leaf-nodes, in the data section. */

uint32 check_index_page(Relation rel, PageHeader header, char *buffer, int block) {
  
	uint32 nerrs = 0;
	BTPageOpaque opaque = NULL;
	
	/* check basic page header */
	nerrs += check_page_header(header, block);
	
	/* (block==0) means it's a meta-page, otherwise it's a regular index-page */
	if (block == BTREE_METAPAGE) {
		
		BTMetaPageData * mpdata = BTPageGetMeta(buffer);
	  
		ereport(DEBUG2, (errmsg("[%d] is a meta-page [magic=%d, version=%d]", block, mpdata->btm_magic, mpdata->btm_version)));
		
		if (mpdata->btm_magic != BTREE_MAGIC) {
			ereport(WARNING,(errmsg("[%d] metapage contains invalid magic number %d (should be %d)", block, mpdata->btm_magic, BTREE_MAGIC)));
			nerrs++;
		}
		
		if (mpdata->btm_version != BTREE_VERSION) {
			ereport(WARNING,(errmsg("[%d] metapage contains invalid version %d (should be %d)", block, mpdata->btm_version, BTREE_VERSION)));
			nerrs++;
		}
		
		/* FIXME Check that the btm_root/btm_fastroot is between 1 and number of index blocks */
		/* FIXME Check that the btm_level/btm_fastlevel is equal to the level fo the root block */
		
	} else {
	  
		opaque = (BTPageOpaque)(buffer + header->pd_special);
	
		/* check there's enough space for index-relevant data */
		if (header->pd_special > BLCKSZ - sizeof(BTPageOpaque)) {
			ereport(WARNING,
					(errmsg("[%d] there's not enough special space for index data (%d > %d)",
							block,
							(int) sizeof(BTPageOpaque),
							BLCKSZ - header->pd_special)));
			nerrs++;
		}
		
		/*
		 * if the page is a leaf page, then level needs to be 0. Otherwise,
		 * it should be > 0. Deleted pages don't have a level, the level
		 * field is interleaved with an xid.
		 */
		if (!P_ISDELETED(opaque))
		{
			if (P_ISLEAF(opaque))
			{
				if (opaque-> btpo.level != 0) {
					ereport(WARNING,
							(errmsg("[%d] is leaf page, but level %d is not zero",
									block, opaque->btpo.level)));
					nerrs++;
				}
			}
			else
			{
				if (opaque-> btpo.level == 0) {
					ereport(WARNING,
							(errmsg("[%d] is a non-leaf page, but level is zero",
									block)));
					nerrs++;
				}
			}
		}
	}
	
	return nerrs;
}

/* checks index tuples on the page, one by one */
uint32 check_index_tuples(Relation rel, PageHeader header, char *buffer, int block) {

	/* tuple checks */
	int ntuples = PageGetMaxOffsetNumber(buffer);
	int i;
	uint32 nerrs = 0;
  
	ereport(DEBUG1, (errmsg("[%d] max number of tuples = %d", block, ntuples)));
	
	/* FIXME check btpo_flags (BTP_LEAF, BTP_ROOT, BTP_DELETED, BTP_META, BTP_HALF_DEAD,
	 *       BTP_SPLIT_END and BTP_HAS_GARBAGE) and act accordingly */
	
	for (i = 0; i < ntuples; i++) {
		/* FIXME this should check lp_flags, just as the heap check */
		nerrs += check_index_tuple(rel, header, block, i, buffer);
	}
	
	if (nerrs > 0) {
		ereport(WARNING, (errmsg("[%d] is probably corrupted, there were %d errors reported", block, nerrs)));
	}
	
	return nerrs;
  
}

/* checks that the tuples do not overlap and then the individual attributes */
/* FIXME This should do exactly the same checks of lp_flags as in heap.c */
uint32 check_index_tuple(Relation rel, PageHeader header, int block, int i, char *buffer) {
  
	uint32 nerrs = 0;
	int j, a, b, c, d;
	
	IndexTuple itup = (IndexTuple)(buffer + header->pd_linp[i].lp_off);
	
	/* FIXME This is used when checking overflowing attributes, but it's not clear what
	 * exactly this means / how it works. Needs a bit more investigation and maybe a review
	 * from soneone who really knows the b-tree implementation. */
	int dlen = IndexTupleSize(itup) - IndexInfoFindDataOffset(itup->t_info);
	
	ereport(DEBUG2,(errmsg("[%d:%d] off=%d len=%d tid=(%d,%d)", block, (i+1),
						   header->pd_linp[i].lp_off, header->pd_linp[i].lp_len,
						   BlockIdGetBlockNumber(&(itup->t_tid.ip_blkid)),
						   itup->t_tid.ip_posid )));
	
	/* check intersection with other tuples */
		  
	/* [A,B] vs [C,D] */
	a = header->pd_linp[i].lp_off;
	b = header->pd_linp[i].lp_off + header->pd_linp[i].lp_len;
	
	ereport(DEBUG2,(errmsg("[%d:%d] checking intersection with other tuples", block, (i+1))));
	
	for (j = 0; j < i; j++) {
	  
		/* FIXME Skip UNUSED/REDIRECT/DEAD tuples */
		if (! (header->pd_linp[i].lp_flags == LP_NORMAL)) {
			ereport(DEBUG3,(errmsg("[%d:%d] skipped (not LP_NORMAL)", block, (j+1))));
			continue;
		}
	  
		c = header->pd_linp[j].lp_off;
		d = header->pd_linp[j].lp_off + header->pd_linp[j].lp_len;

		/* [A,C,B] or [A,D,B] or [C,A,D] or [C,B,D] */
		if (((a < c) && (c < b)) || ((a < d) && (d < b)) ||
			((c < a) && (a < d)) || ((c < b) && (b < d))) {
			ereport(WARNING,(errmsg("[%d:%d] intersects with [%d:%d] (%d,%d) vs. (%d,%d)", block, (i+1), block, j, a, b, c, d)));
			++nerrs;
		}
	}
	
	/* check attributes only for tuples with (lp_flags==LP_NORMAL) */
	if (header->pd_linp[i].lp_flags == LP_NORMAL) {
		nerrs += check_index_tuple_attributes(rel, header, block, i + 1, buffer, dlen);
	}
	
	return nerrs;
	
}

/* checks the individual attributes of the tuple */
uint32 check_index_tuple_attributes(Relation rel, PageHeader header, int block, OffsetNumber offnum, char *buffer, int dlen) {
  
	IndexTuple tuple;
	uint32 nerrs = 0;
	int j, off;
	
	bits8 * bitmap;
	BTPageOpaque opaque;
	ItemId	linp;

	ereport(DEBUG2,(errmsg("[%d:%d] checking attributes for the tuple", block, offnum)));
	
	/* get the index tuple and info about the page */
	linp = &header->pd_linp[offnum - 1];
	tuple = (IndexTuple)(buffer + linp->lp_off);
	opaque = (BTPageOpaque)(buffer + header->pd_special);
	
	/* current attribute offset - always starts at (buffer + off) */
	off = linp->lp_off + IndexInfoFindDataOffset(tuple->t_info);
	
	ereport(DEBUG3,(errmsg("[%d:%d] tuple has %d attributes", block, offnum,
						   RelationGetNumberOfAttributes(rel))));
	/* XXX: MAXALIGN */
	bitmap = (bits8*)(buffer + linp->lp_off + sizeof(IndexTupleData));
	
	/* TODO This is mostly copy'n'paste from check_heap_tuple_attributes,
	   so maybe it could be refactored to share the code. */

	/* For left-most tuples on non-leaf pages, there are no data actually
	   (see src/backend/access/nbtree/README, last paragraph in section "Notes
	   About Data Representation")
   
	   Use P_LEFTMOST/P_ISLEAF to identify such cases (for the leftmost item only)
	   and set len = 0.
	*/

	if (P_LEFTMOST(opaque) && !P_ISLEAF(opaque) && offnum == P_FIRSTDATAKEY(opaque)) {
		ereport(DEBUG3, (errmsg("[%d:%d] leftmost tuple on non-leaf block => no data, skipping", block, offnum)));
		return nerrs;
	}

	/* check all the index attributes */
	for (j = 0; j < rel->rd_att->natts; j++) {
		
		/* default length of the attribute */
		int len = rel->rd_att->attrs[j]->attlen;
		
		/* copy from src/backend/commands/analyze.c */
		bool is_varlena  = (!rel->rd_att->attrs[j]->attbyval && len == -1);
		bool is_varwidth = (!rel->rd_att->attrs[j]->attbyval && len < 0); /* thus it's "len = -2" */
		
		/* if the attribute is marked as NULL (in the tuple header), skip to the next attribute */
		if (IndexTupleHasNulls(tuple) && att_isnull(j, bitmap)) {
			ereport(DEBUG3, (errmsg("[%d:%d] attribute '%s' is NULL (skipping)", block, offnum, rel->rd_att->attrs[j]->attname.data)));
			continue;
		}

		/* fix the alignment (see src/include/access/tupmacs.h) */
		off = att_align_pointer(off, rel->rd_att->attrs[j]->attalign, rel->rd_att->attrs[j]->attlen, buffer+off);
		
		if (is_varlena) { 
		
			/*
			  other interesting macros (see postgres.h) - should do something about those ...
			  
			  VARATT_IS_COMPRESSED(PTR)			VARATT_IS_4B_C(PTR)
			  VARATT_IS_EXTERNAL(PTR)				VARATT_IS_1B_E(PTR)
			  VARATT_IS_SHORT(PTR)				VARATT_IS_1B(PTR)
			  VARATT_IS_EXTENDED(PTR)				(!VARATT_IS_4B_U(PTR))
			*/
			
			len = VARSIZE_ANY(buffer + off);
			
			if (len < 0) {
				ereport(WARNING, (errmsg("[%d:%d] attribute '%s' has negative length < 0 (%d)", block, offnum, rel->rd_att->attrs[j]->attname.data, len)));
				++nerrs;
				break;
			}
			
			if (VARATT_IS_COMPRESSED(buffer + off)) {
				/* the raw length should be less than 1G (and positive) */
				if ((VARRAWSIZE_4B_C(buffer + off) < 0) || (VARRAWSIZE_4B_C(buffer + off) > 1024*1024)) {
					ereport(WARNING, (errmsg("[%d:%d]  attribute '%s' has invalid length %d (should be between 0 and 1G)", block, offnum, rel->rd_att->attrs[j]->attname.data, VARRAWSIZE_4B_C(buffer + off))));
					++nerrs;
					/* no break here, this does not break the page structure - we may check the other attributes */
				}
			}
				
			/* FIXME Check if the varlena value may be detoasted. */
			
		} else if (is_varwidth) {
		
			/* get the C-string length (at most to the end of tuple), +1 as it does not include '\0' at the end */
			/* if the string is not properly terminated, then this returns 'remaining space + 1' so it's detected */
			len = strnlen(buffer + off, linp->lp_off + len + linp->lp_len - off) + 1;
			
		}
			
		/* Check if the length makes sense (is not negative and does not overflow
		 * the tuple end, stop validating the other rows (we don't know where to
		 * continue anyway). */
		
		if ((dlen > 0) && (off + len > (linp->lp_off + linp->lp_len))) {
			ereport(WARNING,
					(errmsg("[%d:%d] attribute '%s' (off=%d len=%d) overflows tuple end (off=%d, len=%d)",
							block, offnum, rel->rd_att->attrs[j]->attname.data,
							off, len, linp->lp_off, linp->lp_len)));
			++nerrs;
			break;
		}
		
		/* skip to the next attribute */
		off += (dlen > 0) ? len : 0;
		
		ereport(DEBUG3,(errmsg("[%d:%d] attribute '%s' len=%d", block, offnum, rel->rd_att->attrs[j]->attname.data, len)));
	}
	
	ereport(DEBUG3,
			(errmsg("[%d:%d] last attribute ends at %d, tuple ends at %d",
					block, offnum, off, linp->lp_off + linp->lp_len)));
	
	/* after the last attribute, the offset should be less than the end of the tuple */
	if (MAXALIGN(off) > linp->lp_off + linp->lp_len) {
		ereport(WARNING,
				(errmsg("[%d:%d] the last attribute ends at %d but the tuple ends at %d",
						block, offnum, off, linp->lp_off + linp->lp_len)));
		++nerrs;
	}
	
	return nerrs;
}
