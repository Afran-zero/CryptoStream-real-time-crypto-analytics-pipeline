-- Module 2 — Schemas. Idempotent; the runner wraps each file in its own tx.
create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;
