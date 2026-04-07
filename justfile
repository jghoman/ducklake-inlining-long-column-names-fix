# Repro for https://github.com/duckdb/ducklake/issues/619

# Run repro against DuckLake 0.3 (DuckDB 1.4.4) — Postgres stays up on port 5432
repro-v03:
    docker compose -f docker-compose-v03.yml up -d postgres
    docker compose -f docker-compose-v03.yml run --rm duckdb || true
    @echo "--- v0.3 repro complete — Postgres available on localhost:5432 ---"

# Run repro against DuckLake 0.4 (DuckDB 1.5.1) — Postgres stays up on port 5433
repro-v04:
    docker compose -f docker-compose-v04.yml up -d postgres
    docker compose -f docker-compose-v04.yml run --rm duckdb || true
    @echo "--- v0.4 repro complete — Postgres available on localhost:5433 ---"

# Run both versions
repro-all: repro-v03 repro-v04

# Connect to v0.3 Postgres
psql-v03:
    psql "host=localhost port=5432 user=ducklake password=ducklake dbname=ducklake"

# Connect to v0.4 Postgres
psql-v04:
    psql "host=localhost port=5433 user=ducklake password=ducklake dbname=ducklake"

# Run workaround against v0.3 (rename column, then flush)
workaround-v03:
    docker compose -f docker-compose-v03.yml run --rm --entrypoint 'duckdb -no-stdin -init /workaround.sql' duckdb

# Run workaround against v0.4 (rename column, then flush)
workaround-v04:
    docker compose -f docker-compose-v04.yml run --rm --entrypoint 'duckdb -no-stdin -init /workaround.sql' duckdb

# Tear down v0.3
down-v03:
    docker compose -f docker-compose-v03.yml down -v

# Tear down v0.4
down-v04:
    docker compose -f docker-compose-v04.yml down -v

# Tear down everything
down: down-v03 down-v04

# Clean slate: tear down + repro both
clean-repro: down repro-all
