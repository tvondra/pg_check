/* $PostgreSQL: pgsql/contrib/pageinspect/uninstall_pageinspect.sql,v 1.5 2009/06/08 16:22:44 tgl Exp $ */

-- Adjust this setting to control where the objects get dropped.
SET search_path = public;

DROP FUNCTION pg_check_table(text, bool);
DROP FUNCTION pg_check_table(text, int, int);
DROP FUNCTION pg_check_index(text);
DROP FUNCTION pg_check_index(text, int, int);
