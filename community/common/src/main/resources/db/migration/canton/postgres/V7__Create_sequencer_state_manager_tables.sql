-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Stores members registered with the sequencer for this domain.
-- This table in addition to the member_indexes table as members can be registered for a period without receiving any events.
create table sequencer_state_manager_members (
    member varchar(300) not null,
    -- Keep track of when the member was added so we can determine whether they're stale regardless of whether they've acknowledged anything
    -- UTC timestamp is stored in microseconds relative to EPOCH
    added_at bigint not null,
    enabled bool not null default true,
    latest_acknowledgement bigint,
    primary key (member)
);

-- Stores events for members that can be read with a sequencer subscription.
create table sequencer_state_manager_events (
    -- Registered member
    member varchar(300) not null,
    foreign key (member) references sequencer_state_manager_members (member),
    -- Counter of the event - the first event for a member will be given value 0
    -- Should be monotonically increasing value for a member however this is not enforced within the database and is left as a responsibility for the sequencer writer
    counter bigint not null check(counter >= 0),
    -- Should be an increasing value for a member however this is also not enforced within the database
    -- UTC timestamp in microseconds relative to EPOCH
    ts bigint not null,
    -- serialized signed serialized deliver event
    content bytea not null,
    -- serialized trace context associated with the event
    trace_context bytea not null,
    -- Both ensures members have only a single row for a counter value and provides an efficient index for reading based on member and key (subscriptions read events for a member between a counter range)
    primary key (member, counter)
);

-- Index to speed up fetching the latest timestamp for a store.
-- Is a little redundant to capture all timestamps in an index as we only need the latest one but doesn't seem to negatively
-- impact writes and significantly improves reading our head state that makes startup on a large events table almost
-- instant.
create index idx_sequencer_state_manager_events_ts on sequencer_state_manager_events (ts desc);

-- inclusive lower bound of when events can be read
-- if empty it means all events from epoch can be read
-- is updated when sequencer is pruned meaning that earlier events can no longer be read (and likely no longer exist)
create table sequencer_state_manager_lower_bound (
    single_row_lock char(1) not null default 'X' primary key check(single_row_lock = 'X'),
    ts bigint not null
);
