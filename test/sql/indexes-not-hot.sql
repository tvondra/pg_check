BEGIN;

CREATE EXTENSION pg_check;

CREATE TABLE test_table (
    id      INT PRIMARY KEY,
    id2     INT
);

INSERT INTO test_table SELECT i, i FROM generate_series(1,10000) s(i);

UPDATE test_table SET id = -id;

SELECT pg_check_table('test_table', true, true);

DROP TABLE test_table;

ROLLBACK;
