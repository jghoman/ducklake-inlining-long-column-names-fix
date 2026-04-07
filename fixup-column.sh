#!/usr/bin/env bash
set -euo pipefail

# Truncates a DuckLake column name to fit Postgres's 63-byte NAMEDATALEN limit.
# Updates ducklake_column in the backing Postgres catalog so generated queries
# match the silently-truncated column names in inlined data tables.
#
# See: https://github.com/duckdb/ducklake/issues/619
#
# Usage: fixup-column.sh [--dry-run] <long-column-name> [pg-connection-string]
#
# Example:
#   ./fixup-column.sh --dry-run "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit" \
#       "host=localhost port=5432 user=ducklake password=ducklake dbname=ducklake"

DRY_RUN=false
if [ "${1:-}" = "--dry-run" ]; then
    DRY_RUN=true
    shift
fi

LONG_NAME="${1:?Usage: fixup-column.sh [--dry-run] <long-column-name> [pg-connection-string]}"
PG_CONN="${2:-host=localhost port=5432 user=ducklake password=ducklake dbname=ducklake}"

# Check byte length, not character length — NAMEDATALEN is 63 bytes.
BYTE_LEN=$(printf '%s' "$LONG_NAME" | wc -c | tr -d ' ')
if [ "$BYTE_LEN" -le 63 ]; then
    echo "Column name is $BYTE_LEN bytes, already within NAMEDATALEN (63). Nothing to fix."
    exit 0
fi

echo "Column name:  $LONG_NAME ($BYTE_LEN bytes)"
if $DRY_RUN; then
    echo "Mode:         DRY RUN"
else
    echo "Mode:         LIVE"
fi
echo ""

FINISH_TXN="COMMIT"
if $DRY_RUN; then
    FINISH_TXN="ROLLBACK"
fi

# Use psql variables for safe interpolation (:'var' is properly quoted by psql).
# The DO block can't access psql variables, so we pass the name via a temp table.
psql "$PG_CONN" -v ON_ERROR_STOP=1 \
    -v long_name="$LONG_NAME" \
    -v finish_txn="$FINISH_TXN" <<'SQL'

SET statement_timeout = '10s';
SET lock_timeout = '5s';

BEGIN;

-- Pass the column name into a temp table so the DO block can access it.
CREATE TEMP TABLE _fixup_param (long_name TEXT NOT NULL);
INSERT INTO _fixup_param VALUES (:'long_name');

-- Backup affected rows before any changes.
CREATE TEMP TABLE _fixup_backup AS
SELECT * FROM ducklake_column
WHERE column_name = (SELECT long_name FROM _fixup_param);

DO $$
DECLARE
    v_long_name   TEXT;
    v_truncated   TEXT;
    v_count       INTEGER;
    conflict      RECORD;
BEGIN
    SELECT long_name INTO STRICT v_long_name FROM _fixup_param;

    -- Truncate to 63 bytes, respecting UTF-8 codepoint boundaries.
    -- This matches how Postgres truncates identifiers (NAMEDATALEN).
    v_truncated := convert_from(
        substring(convert_to(v_long_name, 'UTF8') FROM 1 FOR 63),
        'UTF8');

    -- Verify rows exist.
    SELECT count(*) INTO v_count
    FROM ducklake_column
    WHERE column_name = v_long_name;

    IF v_count = 0 THEN
        RAISE EXCEPTION
            'No rows in ducklake_column match column_name = "%". Check for typos.',
            v_long_name;
    END IF;

    RAISE NOTICE 'Found % ducklake_column row(s) to update.', v_count;
    RAISE NOTICE 'Truncated name: % (% bytes)', v_truncated, octet_length(v_truncated);

    -- Lock target rows to close the TOCTOU gap between collision check and UPDATE.
    PERFORM * FROM ducklake_column
    WHERE column_name = v_long_name
    FOR UPDATE;

    -- Collision check: only flag columns with overlapping snapshot ranges
    -- in the same table. Non-overlapping snapshots can't coexist at query time.
    FOR conflict IN
        SELECT c1.column_id AS src_id, c2.column_id AS conflict_id,
               c1.table_id, c2.column_name AS conflict_name,
               c2.begin_snapshot AS c_begin, c2.end_snapshot AS c_end
        FROM ducklake_column c1
        JOIN ducklake_column c2
            ON  c2.table_id = c1.table_id
            AND c2.column_id != c1.column_id
            AND c2.column_name = v_truncated
            -- Snapshot ranges overlap
            AND (c2.end_snapshot IS NULL OR c2.end_snapshot > c1.begin_snapshot)
            AND (c1.end_snapshot IS NULL OR c1.end_snapshot > c2.begin_snapshot)
        WHERE c1.column_name = v_long_name
    LOOP
        RAISE EXCEPTION
            'Collision: truncated name "%" would conflict with column_id=% in table_id=% (snapshots %-%).',
            v_truncated, conflict.conflict_id, conflict.table_id,
            conflict.c_begin, conflict.c_end;
    END LOOP;

    -- Apply the fix.
    UPDATE ducklake_column
    SET column_name = v_truncated
    WHERE column_name = v_long_name;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Updated % row(s) in ducklake_column.', v_count;
END
$$;

-- Show before/after.
\echo ''
\echo '=== Before / After ==='
SELECT b.state, b.column_id, b.table_id, b.begin_snapshot, b.end_snapshot,
       b.column_name, octet_length(b.column_name) AS name_bytes
FROM (
    SELECT 'BEFORE' AS state, column_id, table_id, begin_snapshot, end_snapshot, column_name
    FROM _fixup_backup
    UNION ALL
    SELECT 'AFTER', c.column_id, c.table_id, c.begin_snapshot, c.end_snapshot, c.column_name
    FROM ducklake_column c
    WHERE (c.column_id, c.table_id) IN (SELECT column_id, table_id FROM _fixup_backup)
) b
ORDER BY b.column_id, b.table_id, b.begin_snapshot, b.state;

:finish_txn;
SQL

echo ""
if $DRY_RUN; then
    echo "DRY RUN complete. No changes were committed."
else
    echo "Done. Column name truncated in ducklake_column catalog."
fi
