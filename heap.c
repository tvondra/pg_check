#include "heap.h"

#include "postgres.h"

#include "access/htup.h"
#include "utils/rel.h"

#include "funcapi.h"

/* checks heap tuples (table) on the page, one by one */
uint32 check_heap_tuples(Relation rel, PageHeader header, char *buffer, int block) {

	/* tuple checks */
	int ntuples = PageGetMaxOffsetNumber(buffer);
	int i;
	uint32 nerrs = 0;
  
	ereport(DEBUG1, (errmsg("[%d] max number of tuples = %d", block, ntuples)));
      
	for (i = 0; i < ntuples; i++) {
		nerrs += check_heap_tuple(rel, header, block, i, buffer);
	}
	
	if (nerrs > 0) {
		ereport(WARNING, (errmsg("[%d] is probably corrupted, there were %d errors reported", block, nerrs)));
	}

	return nerrs;
  
}

/* checks that the tuples do not overlap and then the individual attributes */
uint32 check_heap_tuple(Relation rel, PageHeader header, int block, int i, char *buffer) {
  
	uint32 nerrs = 0;
	int j, a, b, c, d;
	
	/* check length with respect to header->pd_linp[i].lp_flags (unused, normal, redirect, dead) - see page 36 */
	if (header->pd_linp[i].lp_flags == LP_REDIRECT) {
	  
		ereport(DEBUG2,(errmsg("[%d:%d] tuple is LP_REDIRECT", block, (i+1))));
		
		/* FIXME check that the LP_REDIRECT target is OK (exists, not empty) to handle HOT tuples properly */
		/* items with LP_REDIRECT need to be handled differently (lp_off holds the link to the next tuple pointer) */
		if (header->pd_linp[i].lp_len != 0) {
			ereport(WARNING,(errmsg("[%d:%d] tuple with LP_REDIRECT and len != 0 (%d)", block, (i+1), header->pd_linp[i].lp_len)));
			++nerrs;
		}
		
		return nerrs;
	  
	} else if (header->pd_linp[i].lp_flags == LP_UNUSED) {
	  
		ereport(DEBUG2,(errmsg("[%d:%d] tuple is LP_UNUSED", block, (i+1))));
	  
		/* LP_UNUSED => (len = 0) */
		if (header->pd_linp[i].lp_len != 0) {
			ereport(WARNING,(errmsg("[%d:%d] tuple with LP_UNUSED and len != 0 (%d)", block, (i+1), header->pd_linp[i].lp_len)));
			++nerrs;
		}
		
		return nerrs;

	} else if (header->pd_linp[i].lp_flags == LP_DEAD) {
		/* FIXME might have data => how to check it ? */
		ereport(DEBUG2,(errmsg("[%d:%d] tuple is DEAD", block, (i+1))));
		return nerrs;
	} else {
	  
		ereport(DEBUG2,(errmsg("[%d:%d] tuple is LP_NORMAL", block, (i+1))));
	  
		/* check that the values (length and offset) are within reasonable boundaries, (between 0 and BLCKSZ) */
		/* FIXME those checks probably don't make much sense, as lp_off/lp_len are unsigned, but
		 * there are some overflow issues (resulting in invalid memory alloc size and a crash). */
		
		if (header->pd_linp[i].lp_len <= 0) {
			ereport(WARNING,(errmsg("[%d:%d] tuple with length <= 0 (%d)", block, (i+1), header->pd_linp[i].lp_len)));
			++nerrs;
		}

		if (header->pd_linp[i].lp_off <= 0) {
			ereport(WARNING,(errmsg("[%d:%d] tuple with offset <= 0 (%d)", block, (i+1), header->pd_linp[i].lp_off)));
			++nerrs;
		}

		/* position on the page */
		if (header->pd_linp[i].lp_off < header->pd_upper) {
			ereport(WARNING,(errmsg("[%d:%d] tuple with offset - length < upper (%d - %d < %d)", block, (i+1), header->pd_linp[i].lp_off, header->pd_linp[i].lp_len, header->pd_upper)));
			++nerrs;
		}

		if (header->pd_linp[i].lp_off + header->pd_linp[i].lp_len > header->pd_special) {
			ereport(WARNING,(errmsg("[%d:%d] tuple with offset > special (%d > %d)", block, (i+1), header->pd_linp[i].lp_off, header->pd_special)));
			++nerrs;
		}
		
	}
	
	/* check intersection with other tuples */
		  
	/* [A,B] vs [C,D] */
	a = header->pd_linp[i].lp_off;
	b = header->pd_linp[i].lp_off + header->pd_linp[i].lp_len;
	
	for (j = 0; j < i; j++) {
	  
		/* FIXME Skip UNUSED/REDIRECT/DEAD tuples (probably shouldn't skip DEAD ones) */
		if (! (header->pd_linp[i].lp_flags == LP_NORMAL)) {
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
	
	return nerrs + check_heap_tuple_attributes(rel, header, block, i, buffer);
	
}

/* checks the individual attributes of the tuple */
uint32 check_heap_tuple_attributes(Relation rel, PageHeader header, int block, int i, char *buffer) {
	
	HeapTupleHeader tupheader;
	uint32 nerrs = 0;
	int j, off;
	
	ereport(DEBUG2,(errmsg("[%d:%d] checking attributes for the tuple", block, (i+1))));

	/* get the header of the tuple (it starts at the 'lp_off' offset and it's t_hoff long (incl. bitmap)) */
	tupheader = (HeapTupleHeader)(buffer + header->pd_linp[i].lp_off);
	
	/* attribute offset - always starts at (buffer + off) */
	off = header->pd_linp[i].lp_off + tupheader->t_hoff;

	/* XXX: This check is bogus. A heap tuple can have fewer attributes
	 * than specified in the relation, if new columns have been added with
	 * ALTER TABLE
	 */
	if (HeapTupleHeaderGetNatts(tupheader) != rel->rd_att->natts) {
		ereport(WARNING,
				(errmsg("[%d:%d] tuple has %d attributes, not %d as expected",
						block, (i+1),
						HeapTupleHeaderGetNatts(tupheader),
						RelationGetNumberOfAttributes(rel))));
		++nerrs;
	} else {
		int	endoff;
	  
		ereport(DEBUG3,(errmsg("[%d:%d] tuple has %d attributes", block, (i+1), rel->rd_att->natts)));
	  
		/* check all the attributes */
		for (j = 0; j < rel->rd_att->natts; j++) {
		  
			/* default length of the attribute */
			int len = rel->rd_att->attrs[j]->attlen;
			
			/* copied from src/backend/commands/analyze.c */
			bool is_varlena  = (!rel->rd_att->attrs[j]->attbyval && len == -1);
			bool is_varwidth = (!rel->rd_att->attrs[j]->attbyval && len < 0); /* thus "len == -2" */

			/* if the attribute is marked as NULL (in the tuple header), skip to the next attribute */
			if ((tupheader->t_infomask & HEAP_HASNULL) && att_isnull(j, tupheader->t_bits)) {
				ereport(DEBUG3, (errmsg("[%d:%d] attribute '%s' is NULL (skipping)", block, (i+1), rel->rd_att->attrs[j]->attname.data)));
				continue;
			}

			/* fix the alignment */
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
					ereport(WARNING, (errmsg("[%d:%d] attribute '%s' has negative length < 0 (%d)", block, (i+1), rel->rd_att->attrs[j]->attname.data, len)));
					++nerrs;
					break;
				}
				
				if (VARATT_IS_COMPRESSED(buffer + off)) {
					/* the raw length should be less than 1G (and positive) */
					if ((VARRAWSIZE_4B_C(buffer + off) < 0) || (VARRAWSIZE_4B_C(buffer + off) > 1024*1024)) {
						ereport(WARNING, (errmsg("[%d:%d]  attribute '%s' has invalid length %d (should be between 0 and 1G)", block, (i+1), rel->rd_att->attrs[j]->attname.data, VARRAWSIZE_4B_C(buffer + off))));
						++nerrs; // ((toast_pointer).va_extsize < (toast_pointer).va_rawsize - VARHDRSZ)
						/* no break here, this does not break the page structure - we may check the other attributes */
					}
				}
				
				/* FIXME  Check if the varlena value may be detoasted - see heap_tuple_untoast_attr in backend/access/heap/tuptoaster.c. */
				
			} else if (is_varwidth) {
			
				/* get the C-string length (at most to the end of tuple), +1 as it does not include '\0' at the end */
				/* if the string is not properly terminated, then this returns 'remaining space + 1' so it's detected */
				len = strnlen(buffer + off, header->pd_linp[i].lp_off + len + header->pd_linp[i].lp_len - off) + 1;
				
			}
			
			/* Check if the length makes sense (is not negative and does not overflow
			 * the tuple end, stop validating the other rows (we don't know where to
			 * continue anyway). */
			endoff = header->pd_linp[i].lp_off + header->pd_linp[i].lp_len;
			if (off + len > endoff) {
				ereport(WARNING,
						(errmsg("[%d:%d] attribute '%s' (off=%d len=%d) overflows tuple end (off=%d, len=%d)",
								block, (i+1),
								rel->rd_att->attrs[j]->attname.data, off, len,
								header->pd_linp[i].lp_off,
								header->pd_linp[i].lp_len)));
				++nerrs;
				break;
			}
			
			/* skip to the next attribute */
			off += len;
			
			ereport(DEBUG3,(errmsg("[%d:%d] attribute '%s' length=%d", block, (i+1), rel->rd_att->attrs[j]->attname.data, len)));
			
		}
	
		ereport(DEBUG3,(errmsg("[%d:%d] last attribute ends at %d, tuple ends at %d", block, (i+1), off, header->pd_linp[i].lp_off + header->pd_linp[i].lp_len)));
		
		/*
		 * The end of last attribute should fall within the length given in
		 * the line pointer.
		 */
		endoff = header->pd_linp[i].lp_off + header->pd_linp[i].lp_len;
		if (off > endoff) {
			ereport(WARNING,
					(errmsg("[%d:%d] the last attribute ends at %d but the tuple ends at %d",
							block, (i+1), off,
							endoff)));
			++nerrs;
		}
		
	}
	
	return nerrs;

}
