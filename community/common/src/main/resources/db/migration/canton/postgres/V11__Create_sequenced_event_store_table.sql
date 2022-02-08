-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Stores the received sequencer messages
create table sequenced_events (
    -- discriminate between different users of the sequenced events tables
    client integer not null,
    -- Proto serialized signed message
    sequenced_event bytea not null,
    -- Explicit fields to query the messages, which are stored as blobs
    type varchar(3) not null check(type IN ('del', 'err', 'ign')),
    -- Timestamp of the time of change in microsecond precision relative to EPOCH
    ts bigint not null,
    -- Sequencer counter of the time of change
    sequencer_counter bigint not null,
    -- serialized trace context associated with the event
    trace_context bytea not null,
    -- flag to skip problematic events
    ignore boolean not null,
    -- The sequencer ensures that the timestamp is unique
    primary key (client, ts)
);

create unique index idx_sequenced_events_sequencer_counter on sequenced_events(client, sequencer_counter);
