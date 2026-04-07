-- Test: fixup affects multiple tables with the same long column name.
-- Verify no cross-table corruption.

INSTALL ducklake;
INSTALL postgres;
LOAD ducklake;
LOAD postgres;

ATTACH 'ducklake:postgres:dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS lakehouse (
    DATA_PATH '/data/lakehouse',
    DATA_INLINING_ROW_LIMIT 10,
    OVERRIDE_DATA_PATH true
);

ATTACH 'dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS pg (TYPE postgres);

.bail on

-- t1: has inlined data
CREATE TABLE lakehouse.t1 (
    id INTEGER,
    "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit" VARCHAR
);
INSERT INTO lakehouse.t1 VALUES (1, 't1-row1');
INSERT INTO lakehouse.t1 VALUES (2, 't1-row2');

-- t2: has inlined data with same column name
CREATE TABLE lakehouse.t2 (
    id INTEGER,
    "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit" VARCHAR
);
INSERT INTO lakehouse.t2 VALUES (10, 't2-row1');
INSERT INTO lakehouse.t2 VALUES (20, 't2-row2');

-- t3: same column name but NO inlined data (empty table)
CREATE TABLE lakehouse.t3 (
    id INTEGER,
    "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit" VARCHAR
);

.print "--- catalog before fixup ---"
SELECT * FROM postgres_query('pg', $$
    SELECT column_id, table_id, column_name, octet_length(column_name) AS bytes
    FROM ducklake_column
    ORDER BY table_id, column_id
$$);

.print "--- applying fixup via postgres_execute ---"
CALL postgres_execute('pg', $$
    UPDATE ducklake_column
    SET column_name = convert_from(
        substring(convert_to(column_name, 'UTF8') FROM 1 FOR 63),
        'UTF8')
    WHERE octet_length(column_name) > 63
$$);

.print "--- catalog after fixup ---"
SELECT * FROM postgres_query('pg', $$
    SELECT column_id, table_id, column_name, octet_length(column_name) AS bytes
    FROM ducklake_column
    ORDER BY table_id, column_id
$$);

-- Reattach to clear cached metadata
DETACH lakehouse;
ATTACH 'ducklake:postgres:dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS lakehouse (
    DATA_PATH '/data/lakehouse',
    DATA_INLINING_ROW_LIMIT 10,
    OVERRIDE_DATA_PATH true
);

.print "--- t1 query ---"
SELECT * FROM lakehouse.main.t1 ORDER BY id;

.print "--- t2 query ---"
SELECT * FROM lakehouse.main.t2 ORDER BY id;

.print "--- t1 flush ---"
CALL ducklake_flush_inlined_data('lakehouse', schema_name := 'main', table_name := 't1');

.print "--- t2 flush ---"
CALL ducklake_flush_inlined_data('lakehouse', schema_name := 'main', table_name := 't2');

.print "--- t1 after flush ---"
SELECT * FROM lakehouse.main.t1 ORDER BY id;

.print "--- t2 after flush ---"
SELECT * FROM lakehouse.main.t2 ORDER BY id;

.print "--- t3 insert after fixup ---"
INSERT INTO lakehouse.t3 VALUES (100, 't3-row1');
SELECT * FROM lakehouse.main.t3;

.print "--- all good ---"
