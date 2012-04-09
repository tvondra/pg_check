/*-------------------------------------------------------------------------
 *
 * pg_check.c
 *	  Functions to check integrity of database blocks.
 *
 * FIXME Proper nesting of debug messages.
 * FIXME There's a lot of code shared between check_heap_tuple_attributes and
 * check_index_tuple_attributes, so let's share what's possible.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/itup.h"
#include "access/nbtree.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/guc.h"

#include "common.h"
#include "index.h"
#include "heap.h"
#include "item-bitmap.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define BTPageGetOpaque(page) ((BTPageOpaque) PageGetSpecialPointer(page))

/* bitmap format (when pg_check.debug = true) */
static const struct config_enum_entry bitmap_options[] = {
        {"base64", BITMAP_BASE64, false},
        {"hex", BITMAP_HEX, false},
        {"binary", BITMAP_BINARY, false},
        {"none", BITMAP_NONE, false},
        {NULL, 0, false}
};

void        _PG_init(void);

bool	pgcheck_debug;
int		pgcheck_bitmap_format = BITMAP_BINARY;

Datum		pg_check_table(PG_FUNCTION_ARGS);
Datum		pg_check_table_pages(PG_FUNCTION_ARGS);

Datum		pg_check_index(PG_FUNCTION_ARGS);
Datum		pg_check_index_pages(PG_FUNCTION_ARGS);

static uint32	check_table(Oid relid, bool checkIndexes, bool crossCheckIndexes, BlockNumber blockFrom, BlockNumber blockTo, bool blockRangeGiven);

static uint32	check_index(Oid indexOid, BlockNumber blockFrom, BlockNumber blockTo, bool blockRangeGiven);

static uint32	check_index_oid(Oid	indexOid, item_bitmap * bitmap);

/*
 * pg_check_table
 *
 * Checks the selected table (and optionally the indexes), returns number of warnings (issues found).
 */
PG_FUNCTION_INFO_V1(pg_check_table);

Datum
pg_check_table(PG_FUNCTION_ARGS)
{
	Oid		relid	 = PG_GETARG_OID(0);
	bool	checkIndexes = PG_GETARG_BOOL(1);
	bool	crossCheckIndexes = PG_GETARG_BOOL(2);
	uint32	nerrs;

	nerrs = check_table(relid, checkIndexes, crossCheckIndexes, 0, 0, false);

	PG_RETURN_INT32(nerrs);
}

/*
 * pg_check_table_pages
 *
 * Checks the selected pages of a table, returns number of warnings (issues found).
 */
PG_FUNCTION_INFO_V1(pg_check_table_pages);

Datum
pg_check_table_pages(PG_FUNCTION_ARGS)
{
	Oid		relid	 = PG_GETARG_OID(0);
	int64	blkfrom  = PG_GETARG_INT64(1);
	int64	blkto    = PG_GETARG_INT64(2);
	uint32	nerrs;

	if (blkfrom < 0 || blkfrom > MaxBlockNumber)
		ereport(ERROR,
				(errmsg("invalid starting block number")));

	if (blkto < 0 || blkto > MaxBlockNumber)
		ereport(ERROR,
				(errmsg("invalid ending block number")));

	nerrs = check_table(relid, false, false,
						(BlockNumber) blkfrom, (BlockNumber) blkto,
						true);

	PG_RETURN_INT32(nerrs);
}

/*
 * pg_check_index
 *
 * Checks the selected index, returns number of warnings (issues found).
 */
PG_FUNCTION_INFO_V1(pg_check_index);

Datum
pg_check_index(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	uint32	nerrs;

	nerrs = check_index(relid, 0, 0, false);

	PG_RETURN_INT32(nerrs);
}

/*
 * pg_check_index_pages
 *
 * Checks a single page, returns number of warnings (issues found).
 */
PG_FUNCTION_INFO_V1(pg_check_index_pages);

Datum
pg_check_index_pages(PG_FUNCTION_ARGS)
{
	Oid		relid	 = PG_GETARG_OID(0);
	int64	blkfrom  = PG_GETARG_INT64(1);
	int64	blkto    = PG_GETARG_INT64(2);
	uint32	nerrs;

	if (blkfrom < 0 || blkfrom > MaxBlockNumber)
		ereport(ERROR,
				(errmsg("invalid starting block number")));

	if (blkto < 0 || blkto > MaxBlockNumber)
		ereport(ERROR,
				(errmsg("invalid ending block number")));

	nerrs = check_index(relid, (BlockNumber) blkfrom, (BlockNumber) blkto, true);

	PG_RETURN_INT32(nerrs);
}

/*
 * check the table, acquires AccessShareLock
 */
static uint32
check_table(Oid relid, bool checkIndexes, bool crossCheckIndexes,
			BlockNumber blockFrom, BlockNumber blockTo, bool blockRangeGiven)
{
	Relation	rel;       /* relation for the 'relname' */
	char	   *raw_page;  /* raw data of the page */
	Buffer		buf;       /* buffer the page is read into */
	uint32		nerrs = 0; /* number of errors found */
	BlockNumber blkno;     /* current block */
	PageHeader 	header;    /* page header */
	BufferAccessStrategy strategy; /* bulk strategy to avoid polluting cache */
	
	/* used to cross-check heap and indexes */
	bool		bitmap_build = false;	/* true only when block range not given */
	item_bitmap *bitmap_heap = NULL;	/* bitmap data */
	
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pg_check functions"))));

	if (blockRangeGiven && checkIndexes) /* shouldn't happen */
		elog(ERROR, "invalid combination of checkIndexes and a block range");

	/* when cross-checking, a more restrictive lock mode is needed */
	if (crossCheckIndexes) {
		rel = relation_open(relid, ShareRowExclusiveLock);
	} else {
		rel = relation_open(relid, AccessShareLock);
	}

	/* Check that this relation has storage */
	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_TOASTVALUE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("object \"%s\" is not a table",
						RelationGetRelationName(rel))));

	/* Initialize buffer to copy to */
	raw_page = (char *) palloc(BLCKSZ);
	
	if (!blockRangeGiven)
	{
		blockFrom = 0;
		blockTo = RelationGetNumberOfBlocks(rel);
		
		/* build the bitmap only when we need to cross-check */
		if (crossCheckIndexes) {
			/* FIXME this needs exclusive lock on the relation (and indexes) */
			bitmap_build = true;
			bitmap_heap  = bitmap_init(blockTo);
		}
	}

	strategy = GetAccessStrategy(BAS_BULKREAD);
		
	/* Take a verbatim copy of each page, and check them */
	for (blkno = blockFrom; blkno < blockTo; blkno++)
	{
		buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		memcpy(raw_page, BufferGetPage(buf), BLCKSZ);

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buf);
		
		/* Call the 'check' routines - first just the header, then the tuples */
		
		header = (PageHeader)raw_page;
		
		nerrs += check_page_header(header, blkno);
		
		/* FIXME Does that make sense to check the tuples if the page header is corrupted? */
		nerrs += check_heap_tuples(rel, header, raw_page, blkno);

		/* update the bitmap with items from this page (but only when needed) */
		if (bitmap_build) {
			bitmap_add_heap_items(bitmap_heap, header, raw_page, blkno);
		}
		
	}
	
	if (pgcheck_debug) {
		bitmap_print(bitmap_heap, pgcheck_bitmap_format);
	}
	
	/* check indexes */
	if (checkIndexes) {
		List	   *list_of_indexes;
		ListCell   *index;
		
		item_bitmap * bitmap_idx = NULL;
		
		if (bitmap_build) {
			bitmap_idx = bitmap_copy(bitmap_heap);
		}
		
		list_of_indexes = RelationGetIndexList(rel);
		
		foreach(index, list_of_indexes) {
			
			/* reset the bitmap (if needed) */
			if (bitmap_build) {
				bitmap_reset(bitmap_idx);
			}
			
			nerrs += check_index_oid(lfirst_oid(index), bitmap_idx);
			
			/* evaluate the bitmap difference (if needed) */
			if (bitmap_build) {
				
				/* compare the bitmaps */
				int ndiffs = bitmap_compare(bitmap_heap, bitmap_idx);
				
				if (pgcheck_debug) {
					bitmap_print(bitmap_idx, pgcheck_bitmap_format);
				}
				
				if (ndiffs != 0) {
					elog(WARNING, "there are %d differences between the table and the index", ndiffs);
				}
				nerrs += ndiffs;
				
			}
			
		}
		
		if (bitmap_build) {
			bitmap_free(bitmap_idx);
		}

		list_free(list_of_indexes);
	}

	/* release the the heap bitmap */
	if (bitmap_build) {
		bitmap_free(bitmap_heap);
	}

	FreeAccessStrategy(strategy);

	if (crossCheckIndexes) {
		relation_close(rel, ShareRowExclusiveLock);
	} else {
		relation_close(rel, AccessShareLock);
	}

	return nerrs;
}

/*
 * check the index, acquires AccessShareLock
 *
 * FIXME This shares an awful amount of code with check_index function, refactor.
 *
 * This is called only from check_table, so there is no reason to support of checking
 * only a part of the index.
 */
static uint32
check_index_oid(Oid	indexOid, item_bitmap * bitmap)
{
	Relation	rel;       /* relation for the 'relname' */
	char	   *raw_page;  /* raw data of the page */
	Buffer		buf;       /* buffer the page is read into */
	uint32      nerrs = 0; /* number of errors found */
	BlockNumber blkno;     /* current block */
	BlockNumber maxblock;  /* number of blocks of a table */
	PageHeader 	header;    /* page header */
	BufferAccessStrategy strategy; /* bulk strategy to avoid polluting cache */
	
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pg_check functions"))));
	
	/* FIXME maybe we need more strict lock here */
	if (bitmap != NULL) {
		rel = index_open(indexOid, ShareRowExclusiveLock);
	} else {
		rel = index_open(indexOid, AccessShareLock);
	}

	/* Check that this relation has storage */
	if (rel->rd_rel->relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("object \"%s\" is not an index",
						RelationGetRelationName(rel))));
	
	/* We only know how to check b-tree indexes, so ignore anything else */
	if (rel->rd_rel->relam != BTREE_AM_OID)
	{
		relation_close(rel, AccessShareLock);
		return 0;
	}

	elog(NOTICE, "checking index: %s", RelationGetRelationName(rel));

	/* Initialize buffer to copy to */
	raw_page = (char *) palloc(BLCKSZ);

	strategy = GetAccessStrategy(BAS_BULKREAD);

	/* Take a verbatim copies of the pages and check them */
	maxblock = RelationGetNumberOfBlocks(rel);
	for (blkno = 0; blkno < maxblock; blkno++)
	{
		buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		memcpy(raw_page, BufferGetPage(buf), BLCKSZ);

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buf);
		
		/* Call the 'check' routines - first just the header, then the contents. */
		
		header = (PageHeader)raw_page;
		
		nerrs += check_index_page(rel, header, raw_page, blkno);
		
		if (blkno > 0) {
		
			/* FIXME Does that make sense to check the tuples if the page header is corrupted? */
			nerrs += check_index_tuples(rel, header, raw_page, blkno);
			
			/* if this is a leaf page (containing actual pointers to the heap),
			   then update the bitmap */
			if ((bitmap != NULL) && P_ISLEAF(BTPageGetOpaque(raw_page))) {
				nerrs += bitmap_add_index_items(bitmap, header, raw_page, blkno);
			}
			
		}
		
	}

	FreeAccessStrategy(strategy);

	if (bitmap != NULL) {
		relation_close(rel, ShareRowExclusiveLock);
	} else {
		relation_close(rel, AccessShareLock);
	}

	return nerrs;
}

/*
 * check the index, acquires AccessShareLock
 */
static uint32
check_index(Oid indexOid, BlockNumber blockFrom, BlockNumber blockTo,
			bool blockRangeGiven)
{
	Relation	rel;       /* relation for the 'relname' */
	char	   *raw_page;  /* raw data of the page */
	Buffer		buf;       /* buffer the page is read into */
	uint32      nerrs = 0; /* number of errors found */
	BlockNumber blkno;     /* current block */
	PageHeader 	header;    /* page header */
	BufferAccessStrategy strategy; /* bulk strategy to avoid polluting cache */

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pg_check functions"))));

	/* FIXME A more strict lock might be more appropriate. */
	rel = relation_open(indexOid, AccessShareLock);

	/* Check that this relation is a b-tree index */
	if ((rel->rd_rel->relkind != RELKIND_INDEX) || (rel->rd_rel->relam != BTREE_AM_OID)) {
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("object \"%s\" is not a b-tree index",
						RelationGetRelationName(rel))));
	}

	/* Initialize buffer to copy to */
	raw_page = (char *) palloc(BLCKSZ);
	
	/* Take a verbatim copies of the pages and check them */
	if (!blockRangeGiven) {
		blockFrom = 0;
		blockTo = RelationGetNumberOfBlocks(rel);
	}

	strategy = GetAccessStrategy(BAS_BULKREAD);

	for (blkno = blockFrom; blkno < blockTo; blkno++)
	{
		buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		memcpy(raw_page, BufferGetPage(buf), BLCKSZ);

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buf);
		
		/* Call the 'check' routines - first just the header, then the contents. */
		
		header = (PageHeader)raw_page;
		
		nerrs += check_index_page(rel, header, raw_page, blkno);
		
		if (blkno > 0) {
			
			/* FIXME Does that make sense to check the tuples if the page header is corrupted? */
			nerrs += check_index_tuples(rel, header, raw_page, blkno);
			
		}
		
	}

	FreeAccessStrategy(strategy);

	relation_close(rel, AccessShareLock);

	return nerrs;
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
    
    /* Define custom GUC variables. */
    DefineCustomBoolVariable("pg_check.debug",
                              "print debugging info.",
                             NULL,
                             &pgcheck_debug,
                             false,
                             PGC_SUSET,
                             0,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             NULL,
                             NULL);

    DefineCustomEnumVariable("pg_check.bitmap_format",
                             "how to print bitmap when debugging",
                             NULL,
                             &pgcheck_bitmap_format,
                             BITMAP_BINARY,
                             bitmap_options,
                             PGC_SUSET,
                             0,
#if (PG_VERSION_NUM >= 90100)
                             NULL,
#endif
                             NULL,
                             NULL);

    EmitWarningsOnPlaceholders("pg_check");

}
