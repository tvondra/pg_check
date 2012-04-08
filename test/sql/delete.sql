BEGIN;

CREATE EXTENSION pg_check;

-- no index
CREATE TABLE test_table (
    id      INT
);

INSERT INTO test_table SELECT i FROM generate_series(1,10000) s(i);

DELETE FROM test_table WHERE MOD(id, 2) = 0;

SELECT pg_check_table('test_table', true, true);

DROP TABLE test_table;

-- index
CREATE TABLE test_table (
    id      INT PRIMARY KEY
);

INSERT INTO test_table SELECT i FROM generate_series(1,10000) s(i);

DELETE FROM test_table WHERE MOD(id, 2) = 0;

SELECT pg_check_table('test_table', true, true);

DROP TABLE test_table;

ROLLBACK;
