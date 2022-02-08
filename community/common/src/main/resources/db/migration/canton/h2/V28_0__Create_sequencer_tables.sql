-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- we create a unique integer id for each member that is more efficient to use in the events table
-- members can read all events from `registered_ts`
create table sequencer_members (
    member varchar(300) primary key,
    id serial unique,
    registered_ts bigint not null,
    enabled bool not null default true
);

-- stores payloads for send events
-- callers should ensure that a unique id is assigned
create table sequencer_payloads (
    -- expected to be a timestamp assigned using the partitioned timestamp generator based on the writer index
    id bigint primary key,
    -- identifier used by writers to determine on conflicts whether it was the payload they wrote or if another writer
    -- is active with the same writer index (can happen as we use the non-locked unsafe storage method for payloads).
    -- sized to hold a uuid.
    instance_discriminator varchar(36) not null,
    content binary large object not null
);

-- sequencer instances maintain the lowest timestamp for which it is safe to read from
create table sequencer_watermarks (
    node_index integer primary key,
    watermark_ts bigint not null,
    sequencer_online bool not null
);

-- readers periodically write checkpoints that maps the calculated timer to a timestamp/event-id.
-- when a new subscription is requested from a counter the sequencer can use these checkpoints to find the closest
-- timestamp below the given counter to start the subscription from.
create table sequencer_counter_checkpoints (
   member integer not null,
   counter bigint not null,
   ts bigint not null,
   primary key (member, counter)
);

-- record the latest acknowledgement sent by a sequencer client of a member for the latest event they have successfully
-- processed and will not re-read.
create table sequencer_acknowledgements (
    member integer primary key,
    ts bigint not null
);

-- inclusive lower bound of when events can be read
-- if empty it means all events from epoch can be read
-- is updated when sequencer is pruned meaning that earlier events can no longer be read (and likely no longer exist)
create table sequencer_lower_bound (
    single_row_lock char(1) not null default 'X' primary key check(single_row_lock = 'X'),
    ts bigint not null
);