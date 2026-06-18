-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- This is a dummy column we are adding in order to test that adding dev version migrations
-- works properly. DO NOT MOVE THIS TO STABLE
alter table common_node_id add column test_column int not null default 0;

-- Maintains the status of contracts to be sent to the indexer on behalf of Online Party Replication
create table par_party_replication_indexing (
  synchronizer_idx integer not null,
  -- UTC timestamp of the time of change in microsecond precision relative to EPOCH
  ts bigint not null,
  -- contract activation change counter used lexicographically with ts to preserve insertion order for indexing
  change_counter bigint not null,

  -- fields for indexing contract activation changes
  contract_id bytea not null,
  change change_type not null,
  reassignment_counter bigint not null,

  -- primary key represents indexing order
  primary key (synchronizer_idx, ts, change_counter)
);

-- Tracks the latest par_party_replication_indexing.id queued to the indexer
-- used to identify the next batch to queue to the indexer.
create table par_party_replication_indexing_watermarks (
  synchronizer_idx integer not null primary key,
  -- UTC timestamp of the time of change in microsecond precision relative to EPOCH
  ts bigint not null,
  change_counter bigint not null
);

-- Tracks the latest par_party_replication_indexing.id confirmed indexed by the indexer
-- used by crash recovery to resume indexing.
create table par_party_replication_indexed_watermarks (
  synchronizer_idx integer not null primary key,
  -- UTC timestamp of the time of change in microsecond precision relative to EPOCH
  ts bigint not null,
  change_counter bigint not null
);

create or replace view debug.par_party_replication_indexing as
  select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    debug.canton_timestamp(ts) as ts,
    change_counter,
    lower(encode(contract_id, 'hex')) as contract_id,
    change,
    reassignment_counter
  from par_party_replication_indexing;

create or replace view debug.par_party_replication_indexing_watermarks as
  select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    debug.canton_timestamp(ts) as ts,
    change_counter
  from par_party_replication_indexing_watermarks;

create or replace view debug.par_party_replication_indexed_watermarks as
  select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    debug.canton_timestamp(ts) as ts,
    change_counter
  from par_party_replication_indexed_watermarks;

create table par_acs_party_running_digest (
    synchronizer_idx integer not null,
    -- encoded integer for the interned party id and order (local - or remote)
    party_and_order_id integer not null,
    -- change timestamp
    ts bigint not null,
    -- change version (if `ts` is in the same time)
    tie_breaker bigint not null,
    digest bytea,
    -- link to the last version of the digest that has the same party_order_id
    replaces_ts bigint,
    replaces_tie_breaker bigint
);

create or replace view debug.par_acs_party_running_digest as
select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    party_and_order_id,
    debug.canton_timestamp(ts) as ts,
    tie_breaker,
    lower(encode(digest, 'hex')) as digest,
    debug.canton_timestamp(replaces_ts) as replaces_ts,
    replaces_tie_breaker
from par_acs_party_running_digest;

create unique index par_acs_party_running_digest_by_key
    on par_acs_party_running_digest (synchronizer_idx, party_and_order_id, ts desc, tie_breaker desc);

create index par_acs_party_running_digest_by_time
    on par_acs_party_running_digest (synchronizer_idx, ts, tie_breaker, party_and_order_id)
    include (replaces_ts, replaces_tie_breaker);

create table par_acs_participant_running_digest (
    synchronizer_idx integer not null,
    -- interned participant id
    participant_id integer not null,
    -- change timestamp
    ts bigint not null,
    -- change version (if `ts` is in the same time)
    tie_breaker bigint not null,
    digest bytea,
    hashed_digest bytea,
    -- link to the last version of the digest that has the same (counter) participant_id
    replaces_ts bigint,
    replaces_tie_breaker bigint
);

create or replace view debug.par_acs_participant_running_digest as
select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    participant_id,
    debug.canton_timestamp(ts) as ts,
    tie_breaker,
    lower(encode(digest, 'hex')) as digest,
    lower(encode(hashed_digest, 'hex')) as hashed_digest,
    debug.canton_timestamp(replaces_ts) as replaces_ts,
    replaces_tie_breaker
from par_acs_participant_running_digest;

create unique index par_acs_participant_running_digest_by_key
    on par_acs_participant_running_digest (synchronizer_idx, participant_id, ts desc, tie_breaker desc);

create index par_acs_participant_running_digest_by_time
    on par_acs_participant_running_digest (synchronizer_idx, ts, tie_breaker, participant_id)
    include (replaces_ts, replaces_tie_breaker);


create table par_acs_running_digests_checkpoint (
    synchronizer_idx integer not null,
    -- change timestamp
    ts bigint not null,
    -- version (if the change is in the same microsecond)
    tie_breaker bigint not null,
    primary key (synchronizer_idx, ts, tie_breaker)
);

create or replace view debug.par_acs_running_digests_checkpoint as
select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    debug.canton_timestamp(ts) as ts,
    tie_breaker
from par_acs_running_digests_checkpoint;
