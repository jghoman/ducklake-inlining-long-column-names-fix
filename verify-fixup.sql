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

.print "--- query ---"
SELECT * FROM lakehouse.main.test_long_column;

.print "--- flush ---"
CALL ducklake_flush_inlined_data('lakehouse', schema_name := 'main', table_name := 'test_long_column');

.print "--- query after flush ---"
SELECT * FROM lakehouse.main.test_long_column;

.print "--- all good ---"
