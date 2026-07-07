CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS uint128;

BEGIN;

DROP TABLE IF EXISTS journal_checkpoint CASCADE;
DROP TABLE IF EXISTS journal CASCADE;

CREATE TABLE journal (
    shard INTEGER NOT NULL,
    key INTEGER NOT NULL,
    ts int16 NOT NULL,
    value1 BYTEA,
    value2 BYTEA,
    replaces_ts int16,
    PRIMARY KEY (shard, key, ts)
);

CREATE TABLE journal_checkpoint (
    shard INTEGER NOT NULL,
    ts int16 NOT NULL,
    PRIMARY KEY (shard, ts)
);

CREATE INDEX journal_by_time ON journal (shard, ts, key)
  INCLUDE (replaces_ts);

COMMIT;
