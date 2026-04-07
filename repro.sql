-- Repro for https://github.com/duckdb/ducklake/issues/619
-- Data inlining breaks with column names > 64 characters

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

-- Confirm Postgres NAMEDATALEN limit (should return 63)
SELECT * FROM postgres_query('pg', $$ SELECT current_setting('max_identifier_length') $$);

-- Create table with column name exceeding 63 bytes
CREATE TABLE lakehouse.test_long_column (
    id INTEGER,
    "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit" VARCHAR
);

-- Insert rows (inlined since DATA_INLINING_ROW_LIMIT=10)
INSERT INTO lakehouse.test_long_column VALUES (1, 'row 1');
INSERT INTO lakehouse.test_long_column VALUES (2, 'row 2');
INSERT INTO lakehouse.test_long_column VALUES (3, 'row 3');
INSERT INTO lakehouse.test_long_column VALUES (4, 'row 4');
INSERT INTO lakehouse.test_long_column VALUES (5, 'row 5');
INSERT INTO lakehouse.test_long_column VALUES (6, 'row 6');
INSERT INTO lakehouse.test_long_column VALUES (7, 'row 7');
INSERT INTO lakehouse.test_long_column VALUES (8, 'row 8');
INSERT INTO lakehouse.test_long_column VALUES (9, 'row 9');
INSERT INTO lakehouse.test_long_column VALUES (10, 'row 10');
INSERT INTO lakehouse.test_long_column VALUES (11, 'row 11');
INSERT INTO lakehouse.test_long_column VALUES (12, 'row 12');
INSERT INTO lakehouse.test_long_column VALUES (13, 'row 13');
INSERT INTO lakehouse.test_long_column VALUES (14, 'row 14');
INSERT INTO lakehouse.test_long_column VALUES (15, 'row 15');

-- Flush inlined data to parquet files
CALL ducklake_flush_inlined_data('lakehouse', schema_name := 'main', table_name := 'test_long_column');

-- Insert one more row so there's inlined data that must be unioned with flushed data
INSERT INTO lakehouse.test_long_column VALUES (16, 'row 16 - post flush');

-- This query should work (inlined + file data union)
SELECT * FROM lakehouse.main.test_long_column;

-- Now trigger the bug: flush again — DuckLake will try to read inlined data
-- using the full column name, but Postgres truncated it to 63 bytes
CALL ducklake_flush_inlined_data('lakehouse', schema_name := 'main', table_name := 'test_long_column');

-- Also try a direct query referencing the long column name
SELECT "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit"
FROM lakehouse.main.test_long_column;
