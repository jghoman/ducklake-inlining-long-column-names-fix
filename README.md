# DuckLake Inlining + Long Column Names: Repro & Fixup

Reproduction and workaround for [duckdb/ducklake#619](https://github.com/duckdb/ducklake/issues/619) — DuckLake data inlining breaks when column names exceed Postgres's 63-byte `NAMEDATALEN` limit.

## The Bug

DuckLake can inline small amounts of row data directly into Postgres tables instead of flushing to parquet. When a column name exceeds 63 bytes:

1. DuckLake issues `CREATE TABLE ducklake_inlined_data_X_Y(... long_column_name ...)` against Postgres
2. Postgres silently truncates the column name to 63 bytes
3. DuckLake's catalog (`ducklake_column`) stores the full name
4. When DuckLake reads back the inlined data, it generates SQL using the full name
5. Postgres doesn't recognize it → **Binder Error**

```
Binder Error: Failed to read inlined data from DuckLake:
Referenced column "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit"
not found in FROM clause!
Candidate bindings: "this_is_a_very_long_column_name_that_exceeds_sixty_four_charact"
```

Confirmed on both DuckLake 0.3 (DuckDB 1.4.4) and DuckLake 0.4 (DuckDB 1.5.1).

## Quick Start

Prerequisites: Docker, [just](https://github.com/casey/just)

```bash
# Reproduce on DuckLake 0.3
just repro-v03

# Reproduce on DuckLake 0.4
just repro-v04

# Both
just repro-all

# Tear down
just down
```

After running a repro, Postgres stays up for inspection:

| Version | Port | Connect |
|---------|------|---------|
| v0.3 | 5432 | `just psql-v03` |
| v0.4 | 5433 | `just psql-v04` |

## The Fixup

`fixup-column.sh` patches the DuckLake catalog in the backing Postgres to reconcile the column name mismatch. It truncates `ducklake_column.column_name` to 63 bytes so it matches what Postgres actually stored in the inlined data table.

```bash
# Dry run — shows what would change, rolls back
./fixup-column.sh --dry-run "the_long_column_name_here" "host=localhost port=5432 ..."

# Live
./fixup-column.sh "the_long_column_name_here" "host=localhost port=5432 ..."
```

The script:
- Truncates by **bytes** (not characters), matching Postgres's `NAMEDATALEN` behavior
- Detects collisions where two columns in the same table would truncate to the same 63-byte prefix
- Collision detection is snapshot-aware (non-overlapping schema versions don't conflict)
- Locks target rows (`SELECT ... FOR UPDATE`) to prevent TOCTOU races with concurrent DDL
- Creates a backup temp table and shows before/after diff
- Sets `statement_timeout` and `lock_timeout` to avoid hanging
- Fails loudly if no matching rows exist (typo protection)
- Runs the entire operation in a single transaction

### What it changes

Only one table: `ducklake_column.column_name`. All other DuckLake catalog tables reference columns by `column_id`, not by name.

### What it does NOT fix

- **Downstream consumers** — queries, dbt models, views, etc. that reference the full column name will need updating to use the truncated name.
- **Prefix collisions** — if two columns in the same table truncate to the same 63-byte prefix, the script aborts. Those columns can't coexist in a Postgres inlined data table at all. Recovery requires manual intervention or waiting for the upstream fix (truncate + md5 suffix).

## What We Learned

### DuckLake catalog internals

| Table | Purpose |
|-------|---------|
| `ducklake_column` | Column metadata. `column_name` is the **only** place the name string lives. `column_id` is used everywhere else. |
| `ducklake_schema_versions` | Maps `begin_snapshot` → `schema_version`. Increments on schema changes. |
| `ducklake_inlined_data_tables` | Maps `(table_id, schema_version)` → Postgres table name (e.g., `ducklake_inlined_data_1_2`). |
| `ducklake_inlined_data_X_Y` | Actual inlined row data. Column names here are Postgres identifiers subject to `NAMEDATALEN`. |

### Schema evolution with inlined data

When a column is renamed via `ALTER TABLE ... RENAME COLUMN`:

- DuckLake creates a **new** `ducklake_column` row with the new name and a new `begin_snapshot`, closing the old row's `end_snapshot`
- A new inlined data table is created (`ducklake_inlined_data_X_{version+1}`) with the new column name
- The old inlined data table is read using the old column name from the snapshot range
- This means `ALTER TABLE RENAME COLUMN` through DuckLake **does not fix the bug** — old inlined data tables still have the truncated name and DuckLake still reads them with the full name from their snapshot

### Postgres NAMEDATALEN

- Identifiers are silently truncated to 63 bytes (compile-time `NAMEDATALEN` of 64, minus null terminator)
- `left(column_name, 63)` is correct for ASCII names
- For multibyte UTF-8, use `convert_from(substring(convert_to(name, 'UTF8') FROM 1 FOR 63), 'UTF8')` to avoid splitting codepoints
- Two columns that share a 63-byte prefix will collide — Postgres rejects the second one at `CREATE TABLE` time

### Parquet compatibility

Parquet files written after the fixup use the truncated column name. DuckLake resolves parquet columns by `column_id`, not by name, so existing parquet files (which could only have been written before the column exceeded 63 bytes, since the bug blocks flush) continue to read correctly.

## File Inventory

### Core

| File | Purpose |
|------|---------|
| `repro.sql` | SQL script that reproduces the bug end-to-end |
| `fixup-column.sh` | Production fixup script — run against the backing Postgres |
| `justfile` | Task runner for repro/teardown |
| `docker-compose-v03.yml` | Postgres 16 + DuckDB 1.4.4 (DuckLake 0.3) |
| `docker-compose-v04.yml` | Postgres 16 + DuckDB 1.5.1 (DuckLake 0.4) |

### Tests

| File | What it validates |
|------|-------------------|
| `test-parquet-compat.sql` | Fixup doesn't break parquet read/write/flush cycle |
| `test-multi-table.sql` | Fixup works across multiple tables with the same long column |
| `test-fixup.sql` | Basic fixup → query → flush → query cycle |
| `verify-fixup.sql` | Minimal post-fixup smoke test |
| `schema-evolution-test.sql` | How DuckLake handles column renames with inlined data |

## A Better Fix: Stop Using Column Names in Inlined Tables

The planned upstream fix is to truncate long names and append an md5 suffix for uniqueness. But this bug exists because of an unnecessary denormalization: DuckLake embeds column names in Postgres DDL identifiers (the inlined data table column names) even though it already maintains a `ducklake_column` catalog with `column_id` as the primary key.

No user ever queries `ducklake_inlined_data_1_2` directly. These are internal tables. The column names there could just be `col_1`, `col_2`, etc., keyed to `column_id`:

```sql
-- Current (breaks at 63 bytes):
CREATE TABLE ducklake_inlined_data_1_1(
    row_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT,
    id INTEGER,
    this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit VARCHAR
);

-- Proposed (column_id-based, no length issues):
CREATE TABLE ducklake_inlined_data_1_1(
    row_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT,
    col_1 INTEGER,
    col_2 VARCHAR
);
```

This would:

- **Eliminate `NAMEDATALEN` issues entirely** — no truncation, no collisions, no backend-specific identifier limits
- **Make schema evolution cheaper** — column renames become a catalog-only update, no need to create a new inlined data table per schema version
- **Work across catalog backends** — Postgres, MySQL, SQLite all have different identifier length limits; `col_N` fits all of them

The tradeoff is debuggability when inspecting the backing Postgres directly, but that's what the catalog is for. The current design stores the column name in two places (catalog metadata AND Postgres DDL identifiers) and has to keep them in sync across a system boundary with different constraints. Classic denormalization bug.

### Investigation

| File | What it explores |
|------|------------------|
| `inspect.sql` | Catalog state: `ducklake_column` vs inlined data table columns |
| `inspect2.sql` | `ducklake_schema_versions` and `ducklake_inlined_data_tables` mappings |
| `inspect-refs.sql` | Which catalog tables reference column names (answer: only `ducklake_column`) |
| `inspect-schema-evo.sql` | Full catalog state after schema evolution |
| `evo-repro.sql` | Demonstrates prefix collision when adding a second long column |
| `workaround.sql` | Failed attempt to fix via DuckLake DDL (`ALTER RENAME`) |
| `fixup.sql` | Early version of the Postgres-side fixup (superseded by `fixup-column.sh`) |
