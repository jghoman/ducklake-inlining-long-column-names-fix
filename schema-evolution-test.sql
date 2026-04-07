-- Test: does DuckLake rename columns in the inlined data table?

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

CREATE TABLE lakehouse.schema_evo_test (
    id INTEGER,
    original_col VARCHAR
);

INSERT INTO lakehouse.schema_evo_test VALUES (1, 'before rename');
INSERT INTO lakehouse.schema_evo_test VALUES (2, 'before rename');

.print "--- before rename: inlined data table columns ---"
SELECT * FROM postgres_query('pg', $$ SELECT column_name FROM information_schema.columns WHERE table_name LIKE 'ducklake_inlined_data%' ORDER BY table_name, ordinal_position $$);

.print "--- renaming column ---"
ALTER TABLE lakehouse.schema_evo_test RENAME COLUMN original_col TO new_col_name;

.print "--- after rename: inlined data table columns ---"
SELECT * FROM postgres_query('pg', $$ SELECT column_name FROM information_schema.columns WHERE table_name LIKE 'ducklake_inlined_data%' ORDER BY table_name, ordinal_position $$);

.print "--- query after rename ---"
SELECT * FROM lakehouse.schema_evo_test;

.print "--- flush after rename ---"
CALL ducklake_flush_inlined_data('lakehouse', schema_name := 'main', table_name := 'schema_evo_test');

.print "--- query after flush ---"
SELECT * FROM lakehouse.schema_evo_test;
