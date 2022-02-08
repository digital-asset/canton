-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table event_log (
    -- Unique log_id for each instance of the event log.
    -- If positive, it is an index referring to a domain id stored in static_strings.
    -- If zero, it refers to the production participant event log.
    -- If negative, it refers to a participant event log used in tests only.
    log_id int not null,
    local_offset bigint not null,
    primary key (log_id, local_offset),
    -- UTC timestamp of the event in microseconds relative to EPOCH
    ts bigint not null,
    -- sequencer counter corresponding to the underlying request, if there is one
    request_sequencer_counter bigint,
    -- Optional event ID:
    -- If the event is an Update.TransactionAccepted, this is the transaction id prefixed by `T`, to enable efficient lookup
    -- of the domain that the transaction was executed on.
    -- For timely-rejected transaction (not an Update.TransactionAccepted),
    -- this is the message UUID of the SubmissionRequest, prefixed by `M`, the domain ID, and the separator `#`.
    -- NULL if the event is neither an Update.TransactionAccepted nor was created as part of a timely rejection.
    event_id varchar(300),
    -- Optional domain ID:
    -- For timely-rejected transactions (not an Update.TransactionAccepted) in the participant event log,
    -- this is the domain ID to which the transaction was supposed to be submitted.
    -- NULL if this is not a timely rejection in the participant event log.
    associated_domain integer,
    -- LedgerSyncEvent serialized using protobuf
    content bytea not null,
    -- TraceContext is serialized using protobuf
    trace_context bytea not null,
    -- Causality change associated with the event, if there is one
    causality_update bytea
);
-- Not strictly required, but sometimes useful.
create index idx_event_log_timestamp on event_log (log_id, ts);
create unique index idx_event_log_event_id on event_log (event_id);

-- TODO(i4027): merge this into the event_log table once we have enter and leave events
create table transfer_causality_updates (
    -- Corresponds to an entry in the event_log table.
    log_id int not null,
    -- Request counter corresponding to the causality update
    request_counter bigint not null,
    primary key (log_id,request_counter),
    -- The request timestamp corresponding to the causality update
    request_timestamp bigint not null,
    -- The CausalityUpdate, serialized with protobuf
    causality_update bytea not null
);

-- Persist a single, linearized, multi-domain event log for the local participant
create table linearized_event_log (
    -- Global offset
    global_offset bigserial not null primary key,
    -- Corresponds to an entry in the event_log table.
    log_id int not null,
    -- Offset in the event log instance designated by log_id
    local_offset bigint not null,
    constraint foreign_key_event_log foreign key (log_id, local_offset) references event_log(log_id, local_offset) on delete cascade,
    -- The participant's local time when the event was published, in microseconds relative to EPOCH.
    -- Increases monotonically with the global offset
    publication_time bigint not null
);
create unique index idx_linearized_event_log_offset on linearized_event_log (log_id, local_offset);
create index idx_linearized_event_log_publication_time on linearized_event_log (publication_time, global_offset);

-- For a party p on a domain d, store the causal dependencies on other domains introduced at a given request counter
create table per_party_causal_dependencies (
    -- Arbitrary primary key
    id bigserial primary key not null,
    -- The party p
    party_id varchar not null,
    -- The domain of p
    owning_domain_id varchar not null,
    -- The timestamp on owning_domain_id where the causal dependency is introduced
    constraint_ts bigint not null,
    -- The request counter where the causal dependency is introduced, or null if the participant is not connected to owning_domain_id
    request_counter bigint,
    -- If the causal dependency comes from a transfer in, what is the origin domain?
    transfer_origin_domain_if_present varchar,
    -- The domain for the constraint
    domain_id varchar not null,
    -- The timestamp that the p has observed on domain_id
    domain_ts bigint not null
);
-- TODO(i7072): Add indices for this table
