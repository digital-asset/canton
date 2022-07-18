-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE transfers ADD source_protocol_version smallint DEFAULT 2 NOT NULL;

-- The column latest_topology_client_ts denotes the sequencing timestamp of an event
-- addressed to the sequencer's topology client such that
-- there is no update to the domain topology state (by sequencing time) between this timestamp
-- and the last event in the block.
-- NULL if no such timestamp is known, e.g., because this block was added before this column was added.
alter table sequencer_block_height
    add column latest_topology_client_ts bigint;
