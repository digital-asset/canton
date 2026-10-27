-- Note: this file is a repeatable migration, this means that:
--  - This file can be edited safely
--  - It is not tracked under Flyways schema V__ versioning
--  - Flyway still tracks this files application, and will only apply it one time or whenever the file changes
--  - Do not remove this file, as Flyway will raise an error
--  - This file is prefixed with triple _ to take precedence among externally dbConfig's parameter repeatable-migrations-paths
--  - It is only meant to hold idempotent operations like table setting defaults

-- Tables pruned by Ledger API pruning

alter table lapi_filter_activate_stakeholder
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_filter_activate_witness
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_filter_achs_stakeholder
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_events_activate_contract
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_filter_deactivate_stakeholder
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_filter_deactivate_witness
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_events_deactivate_contract
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_filter_various_witness
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_events_various_witnessed
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_update_meta
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_events_acs_commitments
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table lapi_command_completions
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );

alter table par_contracts
    set (
    autovacuum_vacuum_scale_factor = 0.0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_vacuum_cost_delay = 5,
    autovacuum_vacuum_insert_scale_factor = 0.0,
    autovacuum_vacuum_insert_threshold = 100000,
    autovacuum_analyze_scale_factor = 0.0,
    autovacuum_analyze_threshold = 1000000
    );
