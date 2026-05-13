CREATE EXTENSION IF NOT EXISTS pgcrypto;

BEGIN;

DROP TABLE IF EXISTS journal_checkpoint CASCADE;
DROP TABLE IF EXISTS journal CASCADE;

CREATE TABLE journal (
    shard INTEGER NOT NULL,
    key INTEGER NOT NULL,
    ts NUMERIC(39,0),
    value1 BYTEA,
    value2 BYTEA,
    replaces_ts NUMERIC(39,0),
    PRIMARY KEY (shard, key, ts)
);

CREATE TABLE journal_checkpoint (
    shard INTEGER NOT NULL,
    ts NUMERIC(39,0) NOT NULL,
    PRIMARY KEY (shard, ts)
);

CREATE INDEX journal_by_time ON journal (shard, ts, key)
  INCLUDE (replaces_ts);

COMMIT;
