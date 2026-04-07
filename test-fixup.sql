INSTALL ducklake;
INSTALL postgres;
LOAD ducklake;
LOAD postgres;

ATTACH 'dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS pg (TYPE postgres);

.bail on

.print "--- applying fixup ---"
CALL postgres_execute('pg', $$ UPDATE ducklake_column SET column_name = left(column_name, 63) WHERE length(column_name) > 63 $$);

.print "--- catalog after fixup ---"
SELECT * FROM postgres_query('pg', $$ SELECT column_id, column_name, length(column_name) AS len FROM ducklake_column $$);

.print "--- attaching ducklake ---"
ATTACH 'ducklake:postgres:dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS lakehouse (
    DATA_PATH '/data/lakehouse',
    DATA_INLINING_ROW_LIMIT 10,
    OVERRIDE_DATA_PATH true
);

.print "--- query ---"
SELECT * FROM lakehouse.main.test_long_column;

.print "--- flush ---"
CALL ducklake_flush_inlined_data('lakehouse', schema_name := 'main', table_name := 'test_long_column');

.print "--- query after flush ---"
SELECT * FROM lakehouse.main.test_long_column;
