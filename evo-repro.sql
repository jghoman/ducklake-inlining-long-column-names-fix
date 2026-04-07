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

-- Create with a long column, inline some data
CREATE TABLE lakehouse.evo_test (
    id INTEGER,
    "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_alpha" VARCHAR
);

INSERT INTO lakehouse.evo_test VALUES (1, 'v1');
INSERT INTO lakehouse.evo_test VALUES (2, 'v1');

-- Add another long column
ALTER TABLE lakehouse.evo_test ADD COLUMN "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_bravo" VARCHAR;

INSERT INTO lakehouse.evo_test VALUES (3, 'v2', 'v2-bravo');

.print "--- ducklake_column ---"
SELECT * FROM postgres_query('pg', $$ SELECT column_id, begin_snapshot, end_snapshot, column_name, length(column_name) as len FROM ducklake_column ORDER BY column_id $$);

.print "--- ducklake_schema_version ---"
SELECT * FROM postgres_query('pg', $$ SELECT * FROM ducklake_schema_version ORDER BY schema_version_id $$);

.print "--- inlined data tables ---"
SELECT * FROM postgres_query('pg', $$
    SELECT c.table_name, c.column_name, length(c.column_name) as len
    FROM information_schema.columns c
    WHERE c.table_name LIKE 'ducklake_inlined_data%'
    ORDER BY c.table_name, c.ordinal_position
$$);
