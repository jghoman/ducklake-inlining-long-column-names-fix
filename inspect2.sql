INSTALL ducklake;
INSTALL postgres;
LOAD ducklake;
LOAD postgres;

ATTACH 'dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS pg (TYPE postgres);

.bail on

.print "--- ducklake_schema_versions ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_schema_versions $$);

.print "--- ducklake_inlined_data_tables ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_inlined_data_tables $$);

.print "--- ducklake_column (full) ---"
SELECT * FROM postgres_query('pg', $$ SELECT column_id, begin_snapshot, end_snapshot, table_id, column_name FROM ducklake_column ORDER BY column_id, begin_snapshot $$);

.print "--- inlined_data_1_1 columns ---"
SELECT * FROM postgres_query('pg', $$ SELECT column_name FROM information_schema.columns WHERE table_name = 'ducklake_inlined_data_1_1' ORDER BY ordinal_position $$);

.print "--- inlined_data_1_2 columns ---"
SELECT * FROM postgres_query('pg', $$ SELECT column_name FROM information_schema.columns WHERE table_name = 'ducklake_inlined_data_1_2' ORDER BY ordinal_position $$);
