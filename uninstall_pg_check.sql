/* $PostgreSQL: pgsql/contrib/pageinspect/uninstall_pageinspect.sql,v 1.5 2009/06/08 16:22:44 tgl Exp $ */

-- Adjust this setting to control where the objects get dropped.
SET search_path = public;

DROP FUNCTION pg_check_table(regclass, bool);
DROP FUNCTION pg_check_table(regclass, bigint, bigint);
DROP FUNCTION pg_check_index(regclass);
DROP FUNCTION pg_check_index(regclass, bigint, bigint);
