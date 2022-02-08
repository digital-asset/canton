-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- store in-flight submissions
create table in_flight_submission (
    -- hash of the change ID as a hex string
    change_id_hash varchar(300) primary key,

    submission_id varchar(300) null,

    submission_domain varchar(300) not null,
    message_id varchar(300) not null,

    -- Sequencer timestamp after which this submission will not be sequenced any more, in microsecond precision relative to EPOCH
    -- If set, this submission is considered unsequenced.
    sequencing_timeout bigint,
    -- Sequencer counter assigned to this submission.
    -- Must be set iff sequencing_timeout is not set.
    sequencer_counter bigint,
    -- Sequencer timestamp assigned to this submission, in microsecond precision relative to EPOCH
    -- Must be set iff sequencing_timeout is not set.
    sequencing_time bigint,

    -- Tracking data for producing a rejection event.
    -- Optional; omitted if other code paths ensure that an event is produced
    -- Must be null if sequencing_timeout is not set.
    tracking_data bytea,

    trace_context bytea not null
);

create index idx_in_flight_submission_timeout on in_flight_submission (submission_domain, sequencing_timeout);
create index idx_in_flight_submission_sequencing on in_flight_submission (submission_domain, sequencing_time);
create index idx_in_flight_submission_message_id on in_flight_submission (submission_domain, message_id);
