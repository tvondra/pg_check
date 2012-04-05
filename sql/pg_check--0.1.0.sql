-- Adjust this setting to control where the objects get created.
SET search_path = public;

--
-- pg_check_table()
--

CREATE OR REPLACE FUNCTION pg_check_table(table_relation regclass, check_indexes bool)
RETURNS int4
AS '$libdir/pg_check', 'pg_check_table'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_check_table(regclass, bool) IS 'checks consistency of the whole table (and optionally all indexes on it)';

CREATE OR REPLACE FUNCTION pg_check_table(table_relation regclass, block_start bigint, block_end bigint)
RETURNS int4
AS '$libdir/pg_check', 'pg_check_table_pages'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_check_table(regclass, bigint, bigint) IS 'checks consistency of a part of the table (range of pages)';

--
-- pg_check_index()
--

CREATE OR REPLACE FUNCTION pg_check_index(index_relation regclass)
RETURNS int4
AS '$libdir/pg_check', 'pg_check_index'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_check_index(regclass) IS 'checks consistency of the whole index';

CREATE OR REPLACE FUNCTION pg_check_index(index_relation regclass, block_start bigint, block_end bigint)
RETURNS int4
AS '$libdir/pg_check', 'pg_check_index_pages'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_check_index(regclass, bigint, bigint) IS 'checks consistency of a part of the index (range of pages)';
