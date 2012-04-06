BEGIN;

CREATE EXTENSION pg_check;

-- simple table with NULLs
CREATE TABLE test_table (
    id      INT
);

INSERT INTO test_table SELECT NULL FROM generate_series(1,10000) s(i);

CREATE INDEX test_table_index ON test_table (id);

SELECT pg_check_table('test_table', false, false);
SELECT pg_check_table('test_table', true, true);

DROP TABLE test_table;

-- simple table with a PK
CREATE TABLE test_table (
    id      INT PRIMARY KEY
);

INSERT INTO test_table SELECT i FROM generate_series(1,10000) s(i);

SELECT pg_check_table('test_table', false, false);
SELECT pg_check_table('test_table', true, true);

DROP TABLE test_table;

-- a bit more complex table with NULLs
CREATE TABLE test_table (
    id      BIGINT PRIMARY KEY,
    val_a   VARCHAR(10),
    val_b   CHAR(10)
);

INSERT INTO test_table SELECT i, NULL, NULL FROM generate_series(1,10000) s(i);

CREATE INDEX test_table_a_index ON test_table (val_a);
CREATE INDEX test_table_b_index ON test_table (val_b);
CREATE INDEX test_table_ab_index ON test_table (val_a, val_b);

SELECT pg_check_table('test_table', false, false);
SELECT pg_check_table('test_table', true, true);

DROP TABLE test_table;

-- a bit more complex table, no NULLs
CREATE TABLE test_table (
    id      BIGINT PRIMARY KEY,
    val_a   VARCHAR(10),
    val_b   CHAR(10)
);

INSERT INTO test_table SELECT i, substr(md5(i::text), 0, floor(random()*10)::int), substr(md5(i::text), 0, floor(random()*10)::int) FROM generate_series(1,10000) s(i);

CREATE INDEX test_table_a_index ON test_table (val_a);
CREATE INDEX test_table_b_index ON test_table (val_b);
CREATE INDEX test_table_ab_index ON test_table (val_a, val_b);

SELECT pg_check_table('test_table', false, false);
SELECT pg_check_table('test_table', true, true);

DROP TABLE test_table;

-- a bit more complex table, mix of NULLs and not NULLs
CREATE TABLE test_table (
    id      BIGINT PRIMARY KEY,
    val_a   VARCHAR(10),
    val_b   CHAR(10)
);

INSERT INTO test_table SELECT i, NULL, substr(md5(i::text), 0, floor(random()*10)::int) FROM generate_series(1,10000) s(i);

CREATE INDEX test_table_a_index ON test_table (val_a);
CREATE INDEX test_table_b_index ON test_table (val_b);
CREATE INDEX test_table_ab_index ON test_table (val_a, val_b);

SELECT pg_check_table('test_table', false, false);
SELECT pg_check_table('test_table', true, true);

DROP TABLE test_table;

ROLLBACK;
