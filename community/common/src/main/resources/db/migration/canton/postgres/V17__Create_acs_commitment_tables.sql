-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- locally computed ACS commitments to a specific period, counter-participant and domain
create table computed_acs_commitments (
    domain_id int not null,
    counter_participant varchar(300) not null,
    -- UTC timestamp in microseconds relative to EPOCH
    from_exclusive bigint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    to_inclusive bigint not null,
    -- the "raw" cryptographic commitment (AcsCommitment.CommitmentType) in its serialized format
    commitment bytea not null,
    primary key (domain_id, counter_participant, from_exclusive, to_inclusive),
    constraint check_nonempty_interval_computed check(to_inclusive > from_exclusive)
);

-- ACS commitments received from counter-participants
create table received_acs_commitments (
    domain_id int not null,
    -- the counter-participant who sent the commitment
    sender varchar(300) not null,
    -- UTC timestamp in microseconds relative to EPOCH
    from_exclusive bigint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    to_inclusive bigint not null,
    -- the received signed commitment message (including all the other fields) in a serialized form
    signed_commitment bytea not null,
    constraint check_to_after_from check(to_inclusive > from_exclusive)
);

create index full_commitment_idx on received_acs_commitments (domain_id, sender, from_exclusive, to_inclusive);

-- the participants whose remote commitments are outstanding
create table outstanding_acs_commitments (
    domain_id int not null,
    -- UTC timestamp in microseconds relative to EPOCH
    from_exclusive bigint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    to_inclusive bigint not null,
    counter_participant varchar not null,
    constraint check_nonempty_interval_outstanding check(to_inclusive > from_exclusive)
);

create unique index unique_interval_participant on outstanding_acs_commitments(domain_id, counter_participant, from_exclusive, to_inclusive);

create index outstanding_acs_commitments_by_time on outstanding_acs_commitments (domain_id, from_exclusive);

-- the last timestamp for which the commitments have been computed, stored and sent
create table last_computed_acs_commitments (
    domain_id int primary key,
    -- UTC timestamp in microseconds relative to EPOCH
    ts bigint not null
);

-- Stores the snapshot ACS commitments (per stakeholder set)
create table commitment_snapshot (
    domain_id int not null,
    -- A stable reference to a stakeholder set, that doesn't rely on the Protobuf encoding being deterministic
    -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
    stakeholders_hash varchar(300) not null,
    stakeholders bytea not null,
    commitment bytea not null,
    primary key (domain_id, stakeholders_hash)
);

-- Stores the time (along with a tie-breaker) of the ACS commitment snapshot
create table commitment_snapshot_time (
    domain_id int not null,
    -- UTC timestamp in microseconds relative to EPOCH
    ts bigint not null,
    tie_breaker bigint not null,
    primary key (domain_id)
);

-- Remote commitments that were received but could not yet be checked because the local participant is lagging behind
create table commitment_queue (
    domain_id int not null,
    sender varchar(300) not null,
    counter_participant varchar(300) not null,
    -- UTC timestamp in microseconds relative to EPOCH
    from_exclusive bigint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    to_inclusive bigint not null,
    commitment bytea not null,
    commitment_hash varchar(300) not null, -- A shorter hash (SHA-256) of the commitment for the primary key instead of the bytea
    constraint check_nonempty_interval_queue check(to_inclusive > from_exclusive),
    primary key (domain_id, sender, counter_participant, from_exclusive, to_inclusive, commitment_hash)
);

create index commitment_queue_by_time on commitment_queue (domain_id, to_inclusive);
