-- Adjust this setting to control where the objects get created.
SET search_path = public;

--
-- pg_check_table()
--

CREATE OR REPLACE FUNCTION pg_check_table(table_relation regclass, check_indexes bool default true, cross_check bool default true, block_start bigint default null, block_end bigint default null)
RETURNS int4
AS '$libdir/pg_check', 'pg_check_table'
LANGUAGE C;

COMMENT ON FUNCTION pg_check_table(regclass, bool, bool, bigint, bigint) IS 'checks consistency of a part of the table (range of pages) and optionally all indexes on it';

--
-- pg_check_index()
--

CREATE OR REPLACE FUNCTION pg_check_index(index_relation regclass, block_start bigint default null, block_end bigint default null)
RETURNS int4
AS '$libdir/pg_check', 'pg_check_index'
LANGUAGE C;

COMMENT ON FUNCTION pg_check_index(regclass, bigint, bigint) IS 'checks consistency of a part of the index (range of pages)';
