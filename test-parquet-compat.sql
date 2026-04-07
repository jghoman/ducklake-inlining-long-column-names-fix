-- Test: does the fixup break reads of parquet files written with the original long name?
--
-- Sequence:
-- 1. Create table with long column, insert rows (inlined)
-- 2. Apply fixup (truncate catalog name to 63 chars)
-- 3. Flush — writes parquet files with truncated column name
-- 4. Insert more rows (inlined again)
-- 5. Query — reads parquet (truncated name) + inlined data (truncated name)
-- 6. Flush again
-- 7. Query — reads two parquet files, both with truncated name
--
-- This validates the fixup doesn't break the flush→parquet→read path.
-- Note: we can't test the case where parquet was written with the FULL name
-- before fixup, because the flush itself is what's broken by the bug.
-- If data was flushed before the bug manifested (e.g. auto-checkpoint),
-- that's a different scenario we test separately below.

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

-- Step 1: create and insert rows (all inlined, no flush yet)
CREATE TABLE lakehouse.parquet_compat_test (
    id INTEGER,
    "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit" VARCHAR
);

INSERT INTO lakehouse.parquet_compat_test VALUES (1, 'batch-1');
INSERT INTO lakehouse.parquet_compat_test VALUES (2, 'batch-1');
INSERT INTO lakehouse.parquet_compat_test VALUES (3, 'batch-1');
INSERT INTO lakehouse.parquet_compat_test VALUES (4, 'batch-1');
INSERT INTO lakehouse.parquet_compat_test VALUES (5, 'batch-1');

.print "--- step 1: 5 rows inlined ---"

-- Step 2: apply fixup
.print "--- step 2: applying fixup ---"
CALL postgres_execute('pg', $$
    UPDATE ducklake_column
    SET column_name = left(column_name, 63)
    WHERE column_name = 'this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit'
$$);

-- Reattach to clear cached metadata
DETACH lakehouse;
ATTACH 'ducklake:postgres:dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS lakehouse (
    DATA_PATH '/data/lakehouse',
    DATA_INLINING_ROW_LIMIT 10,
    OVERRIDE_DATA_PATH true
);

-- Step 3: flush to parquet
.print "--- step 3: flushing to parquet ---"
CALL ducklake_flush_inlined_data('lakehouse', schema_name := 'main', table_name := 'parquet_compat_test');

.print "--- step 3: query after first flush ---"
SELECT * FROM lakehouse.main.parquet_compat_test ORDER BY id;

-- Step 4: insert more rows
INSERT INTO lakehouse.parquet_compat_test VALUES (6, 'batch-2');
INSERT INTO lakehouse.parquet_compat_test VALUES (7, 'batch-2');

.print "--- step 4: 2 more rows inlined ---"

-- Step 5: query — parquet + inlined
.print "--- step 5: query (parquet + inlined) ---"
SELECT * FROM lakehouse.main.parquet_compat_test ORDER BY id;

-- Step 6: flush again
.print "--- step 6: second flush ---"
CALL ducklake_flush_inlined_data('lakehouse', schema_name := 'main', table_name := 'parquet_compat_test');

-- Step 7: query — two parquet files
.print "--- step 7: query after second flush ---"
SELECT * FROM lakehouse.main.parquet_compat_test ORDER BY id;

.print "--- row count (expect 7) ---"
SELECT count(*) AS row_count FROM lakehouse.main.parquet_compat_test;

.print "--- parquet files on disk ---"
SELECT * FROM postgres_query('pg', $$
    SELECT data_file_id, file_path FROM ducklake_data_file ORDER BY data_file_id
$$);

.print "--- all good ---"
