------------------------- Schema definition with (ts, tie_breaker) bigint tuple  -------------------------
CREATE EXTENSION IF NOT EXISTS pgcrypto;

BEGIN;

DROP TABLE IF EXISTS journal_checkpoint CASCADE;
DROP TABLE IF EXISTS journal CASCADE;

CREATE TABLE journal (
    shard INTEGER NOT NULL,
    key INTEGER NOT NULL,
    ts BIGINT,
    tie_breaker BIGINT, 
    value1 BYTEA,
    value2 BYTEA,
    replaces_ts BIGINT,
    replaces_tie_breaker BIGINT,
    PRIMARY KEY (shard, key, ts)
);

CREATE TABLE journal_checkpoint (
    shard INTEGER NOT NULL,
    ts BIGINT NOT NULL,
    tie_breaker BIGINT NOT NULL,
    PRIMARY KEY (shard, ts)
);

CREATE INDEX journal_by_time ON journal (shard, ts, tie_breaker, key)
  INCLUDE (replaces_ts, replaces_tie_breaker);

COMMIT;
