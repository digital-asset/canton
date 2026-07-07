-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- This is a dummy table we are adding in order to test that adding dev version migrations
-- works properly. DO NOT MOVE THIS TO STABLE
create table dev_migration_test (
  test_column int not null default 0
);

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
    -- ledger offset of the change
    change_offset bigint not null,
    -- record time of the change_offset
    ts bigint not null,
    digest bytea,
    -- link to the last version of the digest that has the same party_order_id
    replaces_offset bigint
);

-- DB counterpart of the Scala decode function
-- in com.digitalasset.canton.participant.store.AcsDigestStore.PartyAndOrder
-- and resolving the party_id to string format
create or replace function debug.decode_party_and_order(integer) returns varchar as -- collate "C" result. This comment is necessary to satisfy our linter
$$
select
    (debug.resolve_lapi_interned_string($1 / 2) ||
    case
        -- If encoded % 2 == 0, it's LocalPartyFirst
        when $1 % 2 = 0 then ' (LocalPartyFirst)'
        -- Otherwise, it's RemotePartyFirst
        else ' (RemotePartyFirst)'
        end) collate "C"; -- collate in the right place because PG doesn't let us use it in the function signature
$$
language sql
  immutable
  returns null on null input;


create or replace view debug.par_acs_party_running_digest as
select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    debug.decode_party_and_order(party_and_order_id) as party_and_order_id,
    change_offset,
    debug.canton_timestamp(ts) as ts,
    lower(encode(digest, 'hex')) as digest,
    replaces_offset
from par_acs_party_running_digest;

create unique index par_acs_party_running_digest_by_key
    on par_acs_party_running_digest (synchronizer_idx, party_and_order_id, change_offset desc);

create index par_acs_party_running_digest_by_time
    on par_acs_party_running_digest (synchronizer_idx, change_offset, party_and_order_id)
    include (replaces_offset);

create table par_acs_participant_running_digest (
    synchronizer_idx integer not null,
    -- interned participant id
    participant_id integer not null,
    -- change's ledger offset
    change_offset bigint not null,
    -- record time of the change_offset
    ts bigint not null,
    digest bytea,
    hashed_digest bytea,
    -- link to the last version of the digest that has the same (counter) participant_id
    replaces_offset bigint
);

create or replace view debug.par_acs_participant_running_digest as
select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    debug.resolve_lapi_interned_string(participant_id) as participant_id,
    change_offset,
    debug.canton_timestamp(ts) as ts,
    lower(encode(digest, 'hex')) as digest,
    lower(encode(hashed_digest, 'hex')) as hashed_digest,
    replaces_offset
from par_acs_participant_running_digest;

create unique index par_acs_participant_running_digest_by_key
    on par_acs_participant_running_digest (synchronizer_idx, participant_id, change_offset desc);

create index par_acs_participant_running_digest_by_time
    on par_acs_participant_running_digest (synchronizer_idx, change_offset, participant_id)
    include (replaces_offset);


create table par_acs_running_digests_checkpoint (
    synchronizer_idx integer not null,
    -- ledger offset
    change_offset bigint not null,
    -- record time of the change_offset
    ts bigint not null,
    primary key (synchronizer_idx, change_offset)
);

create or replace view debug.par_acs_running_digests_checkpoint as
select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    change_offset,
    debug.canton_timestamp(ts) as ts
from par_acs_running_digests_checkpoint;
