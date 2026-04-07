-- Workaround for https://github.com/duckdb/ducklake/issues/619
-- Rename the long column to <=60 chars, then flush.

INSTALL ducklake;
INSTALL postgres;
LOAD ducklake;
LOAD postgres;

ATTACH 'ducklake:postgres:dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS lakehouse (
    DATA_PATH '/data/lakehouse',
    DATA_INLINING_ROW_LIMIT 10,
    OVERRIDE_DATA_PATH true
);

.bail on

-- Rename the column to 55 characters
ALTER TABLE lakehouse.test_long_column
    RENAME COLUMN "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit"
    TO "this_is_a_very_long_column_name_that_exceeds_sixty_char";

.print "--- ALTER succeeded ---"

-- Now try to flush
CALL ducklake_flush_inlined_data('lakehouse', schema_name := 'main', table_name := 'test_long_column');

.print "--- FLUSH succeeded ---"

-- And query
SELECT * FROM lakehouse.main.test_long_column;
