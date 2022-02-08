-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- stores all requests for the request journal
create table journal_requests (
    domain_id integer not null,
    request_counter bigint not null,
    request_state_index smallint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    request_timestamp bigint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    -- is set only if the request is clean
    commit_time bigint,
    repair_context varchar(300), -- only set on manual repair requests outside of sync protocol
    primary key (domain_id, request_counter));
create index idx_journal_request_timestamp on journal_requests (domain_id, request_timestamp);
create index idx_journal_request_commit_time on journal_requests (domain_id, commit_time);

-- the last recorded head clean counter for each domain
create table head_clean_counters (
    client integer not null primary key,
    prehead_counter bigint not null, -- request counter of the prehead request
    -- UTC timestamp in microseconds relative to EPOCH
    ts bigint not null
);