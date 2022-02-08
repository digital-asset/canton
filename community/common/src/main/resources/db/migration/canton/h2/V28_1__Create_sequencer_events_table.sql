-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- h2 events table (differs from postgres in the recipients array definition)
create table sequencer_events (
    ts bigint primary key,
    node_index smallint not null,
    -- single char to indicate the event type: D for deliver event, E for deliver error
    event_type char(1) not null
        constraint event_type_enum check (event_type = 'D' or event_type = 'E'),
    message_id varchar null,
    sender integer null,
    -- null if event goes to everyone, otherwise specify member ids of recipients
    recipients varchar array null,
    -- null if the event is a deliver error
    -- intentionally not creating a foreign key here for performance reasons
    payload_id bigint null,
    -- optional signing timestamp for deliver events
    signing_timestamp bigint null,
    -- optional error message for deliver error
    error_message varchar null,
    -- trace context associated with the event
    trace_context binary large object not null
);
