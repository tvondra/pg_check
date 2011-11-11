-- Adjust this setting to control where the objects get created.
SET search_path = public;

--
-- pg_check_table()
--

CREATE OR REPLACE FUNCTION pg_check_table(regclass, bool)
RETURNS int4
AS '$libdir/pg_check', 'pg_check_table'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION pg_check_table(regclass, bigint, bigint)
RETURNS int4
AS '$libdir/pg_check', 'pg_check_table_pages'
LANGUAGE C STRICT;

--
-- pg_check_index()
--

CREATE OR REPLACE FUNCTION pg_check_index(regclass)
RETURNS int4
AS '$libdir/pg_check', 'pg_check_index'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION pg_check_index(regclass, bigint, bigint)
RETURNS int4
AS '$libdir/pg_check', 'pg_check_index_pages'
LANGUAGE C STRICT;
