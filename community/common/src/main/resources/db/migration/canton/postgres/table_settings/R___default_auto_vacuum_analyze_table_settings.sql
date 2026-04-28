-- Note: this file is a repeatable migration, this means that:
--  - This file can be edited safely
--  - It is not tracked under Flyways schema V__ versioning
--  - Flyway still tracks this files application, and will only apply it one time or whenever the file changes
--  - Do not remove this file, as Flyway will raise an error
--  - This file is prefixed with triple _ to take precedence among externally dbConfig's parameter repeatable-migrations-paths
--  - It is only meant to hold idempotent operations like table setting defaults

-- ====== Sequencer autovacuum settings =====
alter table sequencer_events
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table sequencer_event_recipients
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table sequencer_payloads
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table seq_block_height
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table seq_traffic_control_consumed_journal
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table seq_in_flight_aggregated_sender
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table seq_in_flight_aggregation
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

-- ====== BFT ordering autovacuum settings =====
alter table ord_epochs
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table ord_availability_batch
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table ord_pbft_messages_in_progress
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table ord_pbft_messages_completed
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table ord_metadata_output_blocks
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table ord_metadata_output_epochs
    set (
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );

alter table ord_leader_selection_state
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_freeze_min_age = 1000000,
    autovacuum_freeze_max_age = 600000000,
    autovacuum_freeze_table_age = 600000000
    );
