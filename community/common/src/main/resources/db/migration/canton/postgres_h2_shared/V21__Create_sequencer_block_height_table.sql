-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- this table is meant to be used by blockchain based external sequencers
-- in order to keep track of the blocks processed associating block heights
-- with the timestamp of the last event in it
create table sequencer_block_height (
    height bigint primary key check(height >= -1),
    latest_event_ts bigint not null
);

create table sequencer_initial_state (
    member varchar(300) primary key,
    counter bigint not null
);
