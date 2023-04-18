-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- This is a dummy column we are adding in order to test that adding dev version migrations
-- works properly. DO NOT MOVE THIS TO STABLE
ALTER TABLE node_id ADD COLUMN test_column INT NOT NULL DEFAULT 0;

-- TODO(#12373) Move this to stable when releasing BFT: BEGIN
CREATE TABLE in_flight_aggregation(
    aggregation_id varchar(300) collate "C" not null primary key,
    -- UTC timestamp in microseconds relative to EPOCH
    max_sequencing_time bigint not null,
    -- serialized aggregation rule,
    aggregation_rule bytea not null
);

CREATE INDEX in_flight_aggregation_max_sequencing_time on in_flight_aggregation(max_sequencing_time);

CREATE TABLE in_flight_aggregated_sender(
    aggregation_id varchar(300) collate "C" not null,
    sender varchar(300) collate "C" not null,
    -- UTC timestamp in microseconds relative to EPOCH
    sequencing_timestamp bigint not null,
    signatures bytea not null,
    primary key (aggregation_id, sender),
    constraint foreign_key_in_flight_aggregated_sender foreign key (aggregation_id) references in_flight_aggregation(aggregation_id) on delete cascade
);
-- TODO(#12373) Move this to stable when releasing BFT: END
