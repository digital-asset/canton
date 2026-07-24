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
  trace_data varchar,
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
  trace_data varchar,
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
  checkpoint_type integer not null,
  primary key (synchronizer_idx, change_offset)
);

create table par_acs_commitment_period_outstanding (
  synchronizer_idx integer not null,
  -- interned participant id
  participant_id integer not null,
  from_exclusive bigint not null,
  to_inclusive bigint not null,
  expected_hashed_digest varbinary not null,
  primary key (synchronizer_idx, participant_id, to_inclusive)
);

create index par_acs_commitment_period_outstanding_to_inclusive on par_acs_commitment_period_outstanding (
  synchronizer_idx,
  to_inclusive
);

create table par_acs_commitment_period_mismatch (
  synchronizer_idx integer not null,
  -- interned participant id
  participant_id integer not null,
  from_exclusive bigint not null,
  to_inclusive bigint not null,
  -- The offset of the first mismatching ReceivedAcsCommitment Update
  update_offset bigint not null,
  -- null indicates that this commitment has been unexpected
  expected_hashed_digest varbinary,
  primary key (synchronizer_idx, participant_id, to_inclusive)
);

create index par_acs_commitment_period_mismatch_by_hash ON par_acs_commitment_period_mismatch (
  synchronizer_idx,
  participant_id,
  expected_hashed_digest,
  to_inclusive
);

create index par_acs_commitment_period_mismatch_to_inclusive on par_acs_commitment_period_mismatch (
  synchronizer_idx,
  to_inclusive
);

create table par_acs_commitment_period_match (
  synchronizer_idx integer not null,
  -- interned participant id
  participant_id integer not null,
  from_exclusive bigint not null,
  to_inclusive bigint not null,
  -- The offset of the first matching ReceivedAcsCommitment Update
  update_offset bigint not null,
  primary key (synchronizer_idx, participant_id, to_inclusive)
);

create index par_acs_commitment_period_match_to_inclusive on par_acs_commitment_period_match (
  synchronizer_idx,
  to_inclusive
);

create table par_acs_commitment_period_watermark (
  synchronizer_idx integer not null,
  watermark_reconciliation bigint not null,
  watermark_affirmation bigint not null,
  watermark_matching bigint,
  primary key (synchronizer_idx)
);

create table par_acs_commitment_period_pruning (
  synchronizer_idx integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  succeeded bigint null,
  primary key (synchronizer_idx)
);
