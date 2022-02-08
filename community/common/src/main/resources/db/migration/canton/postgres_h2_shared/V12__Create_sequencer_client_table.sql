-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Track what send requests we've made but have yet to observe being sequenced.
-- If events are not observed by the max sequencing time we know that the send will never be processed.
create table sequencer_client_pending_sends (
    -- ids for distinguishing between different sequencer clients in the same node
    client integer not null,

    -- the message id of the send being tracked (expected to be unique for the sequencer client while the send is in flight)
    message_id varchar(300) not null,

    -- the message id should be unique for the sequencer client
    primary key (client, message_id),

    -- the max sequencing time of the send request (UTC timestamp in microseconds relative to EPOCH)
    max_sequencing_time bigint not null
);