-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table par_acs_party_running_digest (
  synchronizer_idx integer not null,
  -- encoded integer for the interned party id
  party_id integer not null,
  -- ledger offset of the change
  change_offset bigint not null,
  -- record time of the change_offset
  ts bigint not null,
  digest bytea,
  trace_data varchar collate "C",
  -- link to the last version of the digest that has the same party_id
  replaces_offset bigint
);

alter table par_acs_party_running_digest
  alter column digest set storage plain;

create or replace view debug.par_acs_party_running_digest as
select
  debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
  debug.resolve_lapi_interned_string(party_id) as party_id,
  change_offset,
  debug.canton_timestamp(ts) as ts,
  lower(encode(digest, 'hex')) as digest,
  trace_data :: json,
  replaces_offset
from par_acs_party_running_digest;

create unique index par_acs_party_running_digest_by_key
  on par_acs_party_running_digest (synchronizer_idx, party_id, change_offset desc);

create index par_acs_party_running_digest_by_time
  on par_acs_party_running_digest (synchronizer_idx, change_offset, party_id)
  include (replaces_offset);

create index par_acs_party_running_digest_tombstone
  on par_acs_party_running_digest (synchronizer_idx, change_offset)
  where digest is null;

create table par_acs_participant_running_digest (
  synchronizer_idx integer not null,
  -- interned participant id
  participant_id integer not null,
  -- change's ledger offset
  change_offset bigint not null,
  -- record time of the change_offset
  ts bigint not null,
  digest bytea,
  trace_data varchar collate "C",
  -- link to the last version of the digest that has the same (counter) participant_id
  replaces_offset bigint
);

alter table par_acs_participant_running_digest
  alter column digest set storage plain;

create or replace view debug.par_acs_participant_running_digest as
select
  debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
  debug.resolve_lapi_interned_string(participant_id) as participant_id,
  change_offset,
  debug.canton_timestamp(ts) as ts,
  lower(encode(digest, 'hex')) as digest,
  trace_data :: json,
  replaces_offset
from par_acs_participant_running_digest;

create unique index par_acs_participant_running_digest_by_key
  on par_acs_participant_running_digest (synchronizer_idx, participant_id, change_offset desc);

create index par_acs_participant_running_digest_by_time
  on par_acs_participant_running_digest (synchronizer_idx, change_offset, participant_id)
  include (replaces_offset);

create index par_acs_participant_running_digest_tombstone
  on par_acs_participant_running_digest (synchronizer_idx, change_offset)
  where digest is null;

create table par_acs_running_digests_checkpoint (
  synchronizer_idx integer not null,
  -- ledger offset
  change_offset bigint not null,
  -- record time of the change_offset
  ts bigint not null,
  checkpoint_type integer not null,
  primary key (synchronizer_idx, change_offset)
);

create index par_acs_running_digests_checkpoint_by_type
  on par_acs_running_digests_checkpoint (synchronizer_idx, checkpoint_type, change_offset)
  include (ts);


-- convert the integer representation to the name of the digest checkpoint
create or replace function debug.checkpoint_type(integer) returns char as
$$
select
  case
    when $1 = 1 then 'ReconciliationIntervalBoundary'
    when $1 = 2 then 'AffirmationIntervalBoundary'
    when $1 = 3 then 'MaxEventsWithoutCheckpoint'
    when $1 = 4 then 'PartyHostingChange'
    when $1 = 5 then 'Reinitialization'
    when $1 = 6 then 'ReceivedCommitmentCheckpoint'
    else $1::text
    end;
$$
  language sql
  immutable
  returns null on null input;

create or replace view debug.par_acs_running_digests_checkpoint as
select
  debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
  change_offset,
  debug.canton_timestamp(ts) as ts,
  debug.checkpoint_type(checkpoint_type) as checkpoint_type
from par_acs_running_digests_checkpoint;

create table par_acs_running_digests_pruning (
  synchronizer_idx integer not null,
  latest_successful_prune_offset bigint not null,
  primary key (synchronizer_idx)
);

create or replace view debug.par_acs_running_digests_pruning as
select
  debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
  latest_successful_prune_offset
from par_acs_running_digests_pruning;

create table par_acs_commitment_period_outstanding (
  synchronizer_idx integer not null,
  -- interned participant id
  participant_id integer not null,
  from_exclusive bigint not null,
  to_inclusive bigint not null,
  expected_hashed_digest bytea not null,
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
  expected_hashed_digest bytea,
  primary key (synchronizer_idx, participant_id, to_inclusive)
);

create index par_acs_commitment_period_mismatch_by_hash ON par_acs_commitment_period_mismatch (
  synchronizer_idx,
  participant_id,
  expected_hashed_digest,
  to_inclusive
  ) where expected_hashed_digest is not null;

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
  watermark_matching bigint not null,
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

create table par_acs_commitment_sender_watermark (
  synchronizer_idx integer not null,
  watermark_offset bigint not null,
  watermark_timestamp bigint not null,
  primary key (synchronizer_idx)
);

create or replace view debug.par_acs_commitment_period_outstanding as
select
  debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
  debug.resolve_lapi_interned_string(participant_id) as participant_id,
  debug.canton_timestamp(from_exclusive) as from_exclusive,
  debug.canton_timestamp(to_inclusive) as to_inclusive,
  lower(encode(expected_hashed_digest, 'hex')) as expected_hashed_digest
from par_acs_commitment_period_outstanding;

create or replace view debug.par_acs_commitment_period_mismatch as
select
  debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
  debug.resolve_lapi_interned_string(participant_id) as participant_id,
  debug.canton_timestamp(from_exclusive) as from_exclusive,
  debug.canton_timestamp(to_inclusive) as to_inclusive,
  update_offset,
  lower(encode(expected_hashed_digest, 'hex')) as expected_hashed_digest
from par_acs_commitment_period_mismatch;

create or replace view debug.par_acs_commitment_period_match as
select
  debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
  debug.resolve_lapi_interned_string(participant_id) as participant_id,
  debug.canton_timestamp(from_exclusive) as from_exclusive,
  debug.canton_timestamp(to_inclusive) as to_inclusive,
  update_offset
from par_acs_commitment_period_match;

create or replace view debug.par_acs_commitment_period_watermark as
select
  debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
  watermark_matching
from par_acs_commitment_period_watermark;

create or replace view debug.par_acs_commitment_period_pruning as
select
  debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
  phase,
  debug.canton_timestamp(ts) as ts,
  debug.canton_timestamp(succeeded) as succeeded
from par_acs_commitment_period_pruning;

create or replace view debug.par_acs_commitment_sender_watermark as
select
  debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
  watermark_offset,
  debug.canton_timestamp(watermark_timestamp) as watermark_timestamp
from par_acs_commitment_sender_watermark;
