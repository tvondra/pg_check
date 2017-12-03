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

#if (PG_VERSION_NUM >= 90600)
#include "catalog/pg_am.h"
#endif

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

void _PG_init(void);

bool pgcheck_debug;
int pgcheck_bitmap_format = BITMAP_BINARY;

Datum pg_check_table(PG_FUNCTION_ARGS);
Datum pg_check_table_pages(PG_FUNCTION_ARGS);

Datum pg_check_index(PG_FUNCTION_ARGS);
Datum pg_check_index_pages(PG_FUNCTION_ARGS);

static uint32 check_table(Oid relid,
						bool checkIndexes, bool crossCheckIndexes,
						BlockNumber blockFrom, BlockNumber blockTo,
						bool blockRangeGiven);

static uint32 check_index(Oid indexOid,
						BlockNumber blockFrom, BlockNumber blockTo,
						bool blockRangeGiven,
						item_bitmap *bitmap, bool *crossCheck);

/*
 * pg_check_table
 *
 * Checks the selected table (and optionally the indexes), returns number
 * of issues found.
 */
PG_FUNCTION_INFO_V1(pg_check_table);

Datum
pg_check_table(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	bool	checkIndexes = PG_GETARG_BOOL(1);
	bool	crossCheckIndexes = PG_GETARG_BOOL(2);
	uint32	nerrs;

	nerrs = check_table(relid, checkIndexes, crossCheckIndexes, 0, 0, false);

	PG_RETURN_INT32(nerrs);
}

/*
 * pg_check_table_pages
 *
 * Checks the selected range of pages of a table (but not indexes), returns
 * number of issues found.
 */
PG_FUNCTION_INFO_V1(pg_check_table_pages);

Datum
pg_check_table_pages(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	int64	blkfrom  = PG_GETARG_INT64(1);
	int64	blkto = PG_GETARG_INT64(2);
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

	nerrs = check_index(relid, 0, 0, false, NULL, NULL);

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
	Oid		relid = PG_GETARG_OID(0);
	BlockNumber	blkfrom = (BlockNumber) PG_GETARG_INT64(1);
	BlockNumber	blkto = (BlockNumber) PG_GETARG_INT64(2);
	uint32	nerrs;

	if (blkfrom < 0 || blkfrom > MaxBlockNumber)
		ereport(ERROR,
				(errmsg("invalid starting block number")));

	if (blkto < 0 || blkto > MaxBlockNumber)
		ereport(ERROR,
				(errmsg("invalid ending block number")));

	nerrs = check_index(relid, blkfrom, blkto, true, NULL, NULL);

	PG_RETURN_INT32(nerrs);
}

/*
 * Check the table, all indexes on the table, and cross-check indexes.
 *
 * The function acquires ShareRowExclusiveLock or AccessShareLock.The
 * stronger lock (AccessShareLock) is used when cross-check is requested.
 *
 * XXX The index cross-check is only allowed for full table check, but
 * that is not really needed - we can scan indexes and only consider
 * pointers to the specified block range.
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
	item_bitmap *bitmap_heap = NULL;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pg_check functions"))));

	if (blockRangeGiven && checkIndexes) /* shouldn't happen */
		elog(ERROR, "cross-check with indexes not possible with explicit block range");

	/* When cross-checking, a more restrictive lock mode is needed. */
	if (crossCheckIndexes)
		rel = relation_open(relid, ShareRowExclusiveLock);
	else
		rel = relation_open(relid, AccessShareLock);

	/* Check that this relation has storage */
	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_TOASTVALUE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("object \"%s\" is not a table",
						RelationGetRelationName(rel))));

	/* Initialize buffer to copy data to */
	raw_page = (char *) palloc(BLCKSZ);

	if (!blockRangeGiven)
	{
		blockFrom = 0;
		blockTo = RelationGetNumberOfBlocks(rel);

		/* build the bitmap only when we need to cross-check */
		if (crossCheckIndexes)
			bitmap_heap  = bitmap_init(blockTo);
	}

	strategy = GetAccessStrategy(BAS_BULKREAD);

	/* Take a verbatim copy of each page, and check it */
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
		if (bitmap_heap)
			bitmap_add_heap_items(bitmap_heap, header, raw_page, blkno);
	}

	if (pgcheck_debug)
		bitmap_print(bitmap_heap, pgcheck_bitmap_format);

	/* check indexes */
	if (checkIndexes)
	{
		List	   *list_of_indexes;
		ListCell   *index;

		item_bitmap * bitmap_idx = NULL;

		/*
		 * Create a bitmap with the same size as the heap bitmap, which
		 * we will populate for each index.
		 *
		 * XXX this also does memcpy() of the contents, which is not
		 * really necessary.
		 */
		if (bitmap_heap)
			bitmap_idx = bitmap_copy(bitmap_heap);

		list_of_indexes = RelationGetIndexList(rel);

		/*
		 * XXX This should probably cross-check only btree indexes.
		 */
		foreach(index, list_of_indexes)
		{
			bool	cross_check;

			/* reset the bitmap (if needed) */
			if (bitmap_heap)
				bitmap_reset(bitmap_idx);

			nerrs += check_index(lfirst_oid(index), 0, 0, false,
								 bitmap_idx, &cross_check);

			/* evaluate the bitmap difference (if needed) */
			if (bitmap_heap && cross_check)
			{
				/* compare the bitmaps */
				int ndiffs = bitmap_compare(bitmap_heap, bitmap_idx);

				if (pgcheck_debug)
					bitmap_print(bitmap_idx, pgcheck_bitmap_format);

				if (ndiffs != 0)
					elog(WARNING, "there are %d differences between the table and the index", ndiffs);

				nerrs += ndiffs;
			}
		}

		if (bitmap_heap)
			bitmap_free(bitmap_idx);

		list_free(list_of_indexes);
	}

	/* release the the heap bitmap */
	if (bitmap_heap)
		bitmap_free(bitmap_heap);

	FreeAccessStrategy(strategy);

	if (crossCheckIndexes)
		relation_close(rel, ShareRowExclusiveLock);
	else
		relation_close(rel, AccessShareLock);

	return nerrs;
}

/*
 * check the index, acquires AccessShareLock
 */
static uint32
check_index(Oid indexOid, BlockNumber blockFrom, BlockNumber blockTo,
			bool blockRangeGiven, item_bitmap *bitmap, bool *crossCheck)
{
	Relation	rel;		/* relation for the 'relname' */
	char	   *raw_page;	/* raw data of the page */
	Buffer		buf;		/* buffer the page is read into */
	uint32      nerrs = 0;	/* number of errors found */
	BlockNumber blkno;		/* current block */
	PageHeader 	header;		/* page header */
	int			lmode;		/* lock mode */
	BufferAccessStrategy strategy; /* bulk strategy to avoid polluting cache */
	check_page_cb	check_page;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pg_check functions"))));

	/* when a bitmap is provided, use stricted lock mode */
	lmode = (bitmap != NULL) ? ShareRowExclusiveLock : AccessShareLock;

	rel = index_open(indexOid, lmode);

	elog(NOTICE, "checking index: %s", RelationGetRelationName(rel));

	/* Check that this relation is an index */
	if (rel->rd_rel->relkind != RELKIND_INDEX)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("object \"%s\" is not an index",
						RelationGetRelationName(rel))));
	}

	/*
	 * See if we have check methods for this access methods. If we find
	 * no am-specific check methods, we'll still do at least the basic
	 * checks of page format.
	 */
	check_page = lookup_check_method(rel->rd_rel->relam, crossCheck);

	/* Initialize buffer to copy to */
	raw_page = (char *) palloc(BLCKSZ);

	/* Take a verbatim copies of the pages and check them */
	if (!blockRangeGiven)
	{
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

		/*
		 * Call the 'check' routines - first just the header, then the
		 * contents of the page.
		 */
		header = (PageHeader)raw_page;

		nerrs += check_page(rel, header, blkno, raw_page, bitmap);
	}

	FreeAccessStrategy(strategy);

	relation_close(rel, lmode);

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
