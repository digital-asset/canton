-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- These are defaults set based on testing of CN CILR test deployment
-- TODO(#21853): These defaults may not be optimal for private domains
alter table sequencer_counter_checkpoints
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
    );

alter table sequencer_events
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
    );

alter table sequencer_payloads
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
    );

alter table seq_block_height
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
    );

alter table seq_traffic_control_consumed_journal
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
    );

alter table seq_in_flight_aggregated_sender
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
    );

alter table seq_in_flight_aggregation
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
    );
