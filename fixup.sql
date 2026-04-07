-- Fixup: reconcile ducklake_column names with what Postgres actually
-- stored in the inlined data tables (truncated to 63 bytes).
--
-- Run against the backing Postgres directly.
-- This updates the DuckLake catalog so generated queries use the
-- truncated column names that Postgres recognizes.

UPDATE ducklake_column
SET column_name = left(column_name, 63)
WHERE length(column_name) > 63;
