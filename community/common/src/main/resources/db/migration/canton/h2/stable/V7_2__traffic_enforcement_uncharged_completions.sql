-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table par_traffic_enforcement_uncharged_completions
(
    -- Ledger API offset of the completion
    completion_offset bigint   not null primary key,
    -- Hash of the Ledger API change id
    change_id_hash    varchar  not null,
    submission_id     varchar  not null,
    paid_traffic_cost bigint   not null,
    -- Why the completion wasn't charged
    reason            smallint not null,
    -- Shape of the completion's act-as parties
    act_as_category   smallint not null,
    -- Record time of the completion
    timestamp         bigint   not null
);

create index idx_traffic_uncharged_completion_timestamp on par_traffic_enforcement_uncharged_completions (timestamp);

create table par_traffic_enforcement_state
(
    -- For example "last_completion_offset"
    name        varchar not null primary key,
    state_value varchar not null
);
