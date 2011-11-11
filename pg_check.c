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

#include "catalog/namespace.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"

#include "common.h"
#include "index.h"
#include "heap.h"
#include "access/itup.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

Datum		pg_check_table(PG_FUNCTION_ARGS);
Datum		pg_check_table_pages(PG_FUNCTION_ARGS);

Datum		pg_check_index(PG_FUNCTION_ARGS);
Datum		pg_check_index_pages(PG_FUNCTION_ARGS);

static uint32	check_table(Oid relid, bool checkIndexes, BlockNumber blockFrom, BlockNumber blockTo);

static uint32	check_index(Oid indexOid, BlockNumber blockFrom, BlockNumber blockTo);

static uint32	check_index_oid(Oid	indexOid);

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
    uint32      nerrs;

	nerrs = check_table(relid, checkIndexes, 0, -1);

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
	uint32	blkfrom  = PG_GETARG_UINT32(1);
    uint32	blkto    = PG_GETARG_UINT32(2);
    uint32	nerrs;

	nerrs = check_table(relid, false, blkfrom, blkto);

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

	nerrs = check_index(relid, 0, -1);

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
	uint32	blkfrom  = PG_GETARG_UINT32(1);
    uint32	blkto    = PG_GETARG_UINT32(2);
    uint32	nerrs;

	nerrs = check_index(relid, blkfrom, blkto);

	PG_RETURN_INT32(nerrs);
}

/*
 * check the table, acquires AccessShareLock
 *
 * FIXME Should have more thorough checks regarding block range / checkIndexes.
 *
 */
static uint32
check_table(Oid relid, bool checkIndexes, BlockNumber blockFrom, BlockNumber blockTo)
{
	Relation	rel;       /* relation for the 'relname' */
	char	   *raw_page;  /* raw data of the page */
	Buffer		buf;       /* buffer the page is read into */
    uint32      nerrs = 0; /* number of errors found */
	BlockNumber blkno;     /* current block */
	PageHeader 	header;    /* page header */
	
	List		*list_of_indexes = NULL;	//list of indexes
	
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pg_check functions"))));
				 
	if (blockFrom < 0)
		ereport(ERROR,
				(errmsg("invalid starting block number %d", blockFrom)));
				
	if ((checkIndexes) && ((blockFrom != 0) || (blockTo != -1)))
		ereport(ERROR,
				(errmsg("invalid combination of checkIndexes, block range")));
	
	/* FIXME is this lock mode sufficient? */
	rel = relation_open(relid, AccessShareLock);

	/* Check that this relation has storage */
	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("object \"%s\" is not a table",
						RelationGetRelationName(rel))));

	/* Initialize buffer to copy to */
	raw_page = (char *) palloc(BLCKSZ);
	
	if (blockTo == -1) {
		blockTo = RelationGetNumberOfBlocks(rel) - 1;
	}
		
	/* Take a verbatim copies of the pages and check them */
	for (blkno = blockFrom; blkno <= blockTo; blkno++) {
	
		/* FIXME Does this use the small circular buffer just like sequential
		 * scan? If not, then it should, otherwise the cache might be polluted
		 * when checking large tables. */
		
		buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		memcpy(raw_page, BufferGetPage(buf), BLCKSZ);

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buf);
		
		/* Call the 'check' routines - first just the header, then the tuples */
		
		header = (PageHeader)raw_page;
		
		nerrs += check_page_header(header, blkno);
		
		/* FIXME Does that make sense to check the tuples if the page header is corrupted? */
		nerrs += check_heap_tuples(rel, header, raw_page, blkno);
		
	}
	
	/* check indexes */
	if (checkIndexes) {
	  
		list_of_indexes = RelationGetIndexList(rel);
		if (list_of_indexes != NULL) {
			ListCell	*index;

			char * index_name;
			Oid index_oid;
		
			foreach(index, list_of_indexes) {
				index_oid=lfirst_oid(index);
				index_name = (char *) get_rel_name(index_oid);
				elog(NOTICE, "checking index: %s", index_name);
				nerrs += check_index_oid(index_oid);
			}
		}
		
		list_free(list_of_indexes);
		
	}

	relation_close(rel, AccessShareLock);

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
check_index_oid(Oid	indexOid)
{
	Relation	rel;       /* relation for the 'relname' */
	char	   *raw_page;  /* raw data of the page */
	Buffer		buf;       /* buffer the page is read into */
    uint32      nerrs = 0; /* number of errors found */
	BlockNumber blkno;     /* current block */
	BlockNumber maxblock;  /* number of blocks of a table */
	PageHeader 	header;    /* page header */

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pg_check functions"))));
	
	/* FIXME maybe we need more strict lock here */
	rel = index_open(indexOid, AccessShareLock);

	/* Check that this relation has storage */
	if (rel->rd_rel->relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("object \"%s\" is not an index",
						RelationGetRelationName(rel))));

	/* Initialize buffer to copy to */
	raw_page = (char *) palloc(BLCKSZ);
	
	/* Take a verbatim copies of the pages and check them */
	maxblock = RelationGetNumberOfBlocks(rel);
	for (blkno = 0; blkno < maxblock; blkno++) {
	
		/* FIXME Does this use the small circular buffer just like sequential
		 * scan? If not, then it should, otherwise the cache might be polluted
		 * when checking large tables. */
		
		buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);
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

	relation_close(rel, AccessShareLock);

	return nerrs;
}

/*
 * check the index, acquires AccessShareLock
 */
static uint32
check_index(Oid indexOid, BlockNumber blockFrom, BlockNumber blockTo)
{
	Relation	rel;       /* relation for the 'relname' */
	char	   *raw_page;  /* raw data of the page */
	Buffer		buf;       /* buffer the page is read into */
    uint32      nerrs = 0; /* number of errors found */
	BlockNumber blkno;     /* current block */
	PageHeader 	header;    /* page header */

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use pg_check functions"))));
	
	if (blockFrom < 0)
		ereport(ERROR,
				(errmsg("invalid starting block number %d", blockFrom)));

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
	if (blockTo == -1) {
		blockTo = RelationGetNumberOfBlocks(rel) - 1;
	}
	
	for (blkno = blockFrom; blkno <= blockTo; blkno++) {
	
		/* FIXME Does this use the small circular buffer just like sequential
		 * scan? If not, then it should, otherwise the cache might be polluted
		 * when checking large tables. */
		
		buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);
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

	relation_close(rel, AccessShareLock);

	return nerrs;
}
