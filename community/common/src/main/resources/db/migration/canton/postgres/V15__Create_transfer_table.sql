-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table transfers (
    -- transfer id
    target_domain varchar(300) not null,
    origin_domain varchar(300) not null,
    -- UTC timestamp in microseconds relative to EPOCH
    request_timestamp bigint not null,
    primary key (target_domain, origin_domain, request_timestamp),

    -- transfer data

    -- UTC timestamp in microseconds relative to EPOCH
    transfer_out_timestamp bigint not null,
    transfer_out_request_counter bigint not null,
    transfer_out_request bytea not null,
    -- UTC timestamp in microseconds relative to EPOCH
    transfer_out_decision_time bigint not null,
    contract bytea not null,
    creating_transaction_id bytea not null,
    transfer_out_result bytea,
    submitter_lf varchar(300) not null,

    -- defined if transfer was completed
    time_of_completion_request_counter bigint,
    -- UTC timestamp in microseconds relative to EPOCH
    time_of_completion_timestamp bigint
);
