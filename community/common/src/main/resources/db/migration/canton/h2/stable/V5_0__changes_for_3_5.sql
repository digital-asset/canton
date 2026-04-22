-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop table common_party_metadata;

alter table seq_block_height add column latest_pending_topology_ts bigint;

alter table lapi_users add column primary_party_authentication boolean not null default false;

-- Backfill existing rows to is_topology_initialized = true
alter table par_static_synchronizer_parameters
    add column is_topology_initialized boolean not null default true;

-- new rows default to is_topology_initialized = false
alter table par_static_synchronizer_parameters
    alter column is_topology_initialized set default false;

ALTER TABLE par_static_synchronizer_parameters RENAME TO par_synchronizer_connectivity_status;

create table lapi_pruning_candidate_deactivated (
    deactivate_event_sequential_id bigint not null,
    activate_event_sequential_id bigint
);
create index lapi_pruning_candidate_deactivated_deactivate_idx ON lapi_pruning_candidate_deactivated USING btree (deactivate_event_sequential_id);
create index lapi_pruning_candidate_deactivated_activate_idx ON lapi_pruning_candidate_deactivated USING btree (activate_event_sequential_id);

create table lapi_pruning_contract_candidate (
    internal_contract_id bigint not null
);
create index lapi_pruning_contract_candidate_idx ON lapi_pruning_contract_candidate USING btree (internal_contract_id);
