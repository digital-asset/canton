-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE event_log DROP COLUMN causality_update;
DROP TABLE per_party_causal_dependencies;

ALTER TABLE transfers DROP PRIMARY KEY;
ALTER TABLE transfers DROP COLUMN request_timestamp;
ALTER TABLE transfers ADD PRIMARY KEY (target_domain, origin_domain, transfer_out_timestamp);
ALTER TABLE transfers ADD COLUMN transfer_out_global_offset bigint;
ALTER TABLE transfers ADD COLUMN transfer_in_global_offset bigint;

ALTER TABLE active_contracts ADD COLUMN transfer_counter bigint default null;

CREATE TABLE fresh_submitted_transaction (
    domain_id integer not null,
    root_hash_hex varchar(300) not null,
    request_id bigint not null,
    max_sequencing_time bigint not null,
    primary key (domain_id, root_hash_hex)
);

CREATE TABLE fresh_submitted_transaction_pruning (
    domain_id integer not null,
    phase pruning_phase not null,
    -- UTC timestamp in microseconds relative to EPOCH
    ts bigint not null,
    primary key (domain_id)
);
