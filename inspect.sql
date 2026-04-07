INSTALL ducklake;
INSTALL postgres;
LOAD ducklake;
LOAD postgres;

ATTACH 'dbname=ducklake host=postgres port=5432 user=ducklake password=ducklake' AS pg (TYPE postgres);

.bail on

.print "--- ducklake_column ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_column $$);

.print "--- inlined data table columns vs catalog ---"
SELECT * FROM postgres_query('pg', $$
    SELECT c.column_name AS pg_column, length(c.column_name) AS pg_len
    FROM information_schema.columns c
    WHERE c.table_name LIKE 'ducklake_inlined_data%'
    AND c.column_name NOT IN ('row_id','begin_snapshot','end_snapshot')
    ORDER BY c.table_name, c.ordinal_position
$$);

.print "--- ducklake_column names ---"
SELECT * FROM postgres_query('pg', $$
    SELECT column_id, name, length(name) AS name_len FROM ducklake_column
$$);
