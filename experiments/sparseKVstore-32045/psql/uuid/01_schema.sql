------------------------- UUID backed Schema definition -------------------------
CREATE EXTENSION IF NOT EXISTS pgcrypto; -- this is required only for data generation

BEGIN;

DROP TABLE IF EXISTS journal_checkpoint CASCADE;
DROP TABLE IF EXISTS journal CASCADE;

CREATE TABLE journal (
    shard INTEGER NOT NULL,
    key INTEGER NOT NULL,
    -- Upper 8 bytes: ts | Lower 8 bytes: tie_breaker
    ts UUID,
    value1 BYTEA,
    value2 BYTEA,
    replaces_ts UUID,
    PRIMARY KEY (shard, key, ts)
);

CREATE TABLE journal_checkpoint (
    shard INTEGER NOT NULL,
    ts UUID NOT NULL,
    PRIMARY KEY (shard, ts)
);

CREATE INDEX journal_by_time ON journal (shard, ts, key)
  INCLUDE (replaces_ts);

COMMIT;
