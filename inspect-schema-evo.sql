INSTALL ducklake;
INSTALL postgres;
LOAD ducklake;
LOAD postgres;

ATTACH 'dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS pg (TYPE postgres);

.bail on

.print "--- all ducklake tables ---"
SELECT * FROM postgres_query('pg', $$ SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'ducklake%' ORDER BY table_name $$);

.print "--- ducklake_column ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_column ORDER BY column_id $$);

.print "--- ducklake_table ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_table $$);

.print "--- ducklake_schema_version (if exists) ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_schema_version $$);

.print "--- ducklake_inlined_data tables ---"
SELECT * FROM postgres_query('pg', $$
    SELECT c.table_name, c.column_name, c.ordinal_position
    FROM information_schema.columns c
    WHERE c.table_name LIKE 'ducklake_inlined_data%'
    ORDER BY c.table_name, c.ordinal_position
$$);
