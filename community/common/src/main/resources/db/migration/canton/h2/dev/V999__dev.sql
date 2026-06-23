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
  contract_id binary varying not null,
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

create table par_acs_party_running_digest (
  synchronizer_idx integer not null,
  -- encoded integer for the interned party id and order (local - or remote)
  party_and_order_id integer not null,
  -- ledger offset
  change_offset bigint not null,
  -- record time of the change_offset
  ts bigint not null,
  -- VARBINARY has a performance and memory layout advantage over BLOB for small, fixed-size byte structures (like digests).
  -- see https://www.h2database.com/html/datatypes.html for details
  digest varbinary,
  -- link to the last version of the digest that has the same party_and_order_id
  replaces_offset bigint,
  primary key (synchronizer_idx, party_and_order_id, change_offset)
);

create index par_acs_party_running_digest_by_time
    on par_acs_party_running_digest (synchronizer_idx, party_and_order_id, change_offset desc, replaces_offset);

create table par_acs_participant_running_digest (
  synchronizer_idx integer not null,
  -- interned participant id
  participant_id integer not null,
  -- ledger offset
  change_offset bigint not null,
  -- record time of the change_offset
  ts bigint not null,
  -- VARBINARY has a performance and memory layout advantage over BLOB for small, fixed-size byte structures (like digests).
  -- see https://www.h2database.com/html/datatypes.html for details
  digest varbinary,
  hashed_digest varbinary,
  -- link to the last version of the digest that has the same (counter) participant_id
  replaces_offset bigint,
  primary key (synchronizer_idx, participant_id, change_offset)
);

create index par_acs_participant_running_digest_by_time
    on par_acs_participant_running_digest (synchronizer_idx, participant_id, change_offset desc, replaces_offset);

create table par_acs_running_digests_checkpoint (
  synchronizer_idx integer not null,
  -- ledger offset
  change_offset bigint not null,
  -- record time of the change_offset
  ts bigint not null,
  primary key (synchronizer_idx, change_offset)
);
