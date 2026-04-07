INSTALL ducklake;
INSTALL postgres;
LOAD ducklake;
LOAD postgres;

ATTACH 'dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS pg (TYPE postgres);

.bail on

.print "--- ducklake_column_mapping ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_column_mapping $$);

.print "--- ducklake_name_mapping ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_name_mapping $$);

.print "--- ducklake_file_column_stats ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_file_column_stats $$);

.print "--- ducklake_table_column_stats ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_table_column_stats $$);

.print "--- ducklake_snapshot_changes ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_snapshot_changes $$);
