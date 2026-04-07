We're going to reproduce this bug:
https://github.com/duckdb/ducklake/issues/619

We want to try and reproduce it in both ducklake 0.3 and 0.4

Write docker-composes for both versions.
1. Stand up an RDS-backed ducklake.
2. Enable data inlining.
3. Create a table with column names > 64 characters.
4. Write some data, which should get inlined.
5. Call checkpoint.  Expect problems.

Use a justfile to coordinate all of this.
