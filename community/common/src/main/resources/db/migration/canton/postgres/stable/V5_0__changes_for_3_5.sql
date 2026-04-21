-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop view debug.common_party_metadata;
drop table common_party_metadata;

alter table seq_block_height add column latest_pending_topology_ts bigint;

create or replace view debug.seq_block_height as
select
    height,
    debug.canton_timestamp(latest_event_ts) as latest_event_ts,
    debug.canton_timestamp(latest_sequencer_event_ts) as latest_sequencer_event_ts,
    debug.canton_timestamp(latest_pending_topology_ts) as latest_pending_topology_ts
from seq_block_height;

alter table lapi_users add column primary_party_authentication boolean not null default false;
create or replace view debug.lapi_users as
  select
    internal_id,
    user_id,
    primary_party,
    debug.canton_timestamp(created_at) as created_at,
    is_deactivated,
    resource_version,
    identity_provider_id,
    primary_party_authentication
  from lapi_users;

-- Backfill existing rows to is_topology_initialized = true
alter table par_static_synchronizer_parameters add column is_topology_initialized boolean not null default true;

-- new rows default to is_topology_initialized = false
alter table par_static_synchronizer_parameters alter column is_topology_initialized set default false;

create or replace view debug.par_static_synchronizer_parameters as
select
    physical_synchronizer_id,
    params,
    is_topology_initialized
from par_static_synchronizer_parameters;

ALTER TABLE par_static_synchronizer_parameters RENAME TO par_synchronizer_connectivity_status;

create or replace view debug.par_synchronizer_connectivity_status as
select
    physical_synchronizer_id,
    params,
    is_topology_initialized
from par_synchronizer_connectivity_status;

drop view if exists debug.par_static_synchronizer_parameters;

create table lapi_pruning_candidate_deactivated (
    deactivate_event_sequential_id bigint not null,
    activate_event_sequential_id bigint
);
create index lapi_pruning_candidate_deactivated_deactivate_idx ON lapi_pruning_candidate_deactivated USING btree (deactivate_event_sequential_id);
create index lapi_pruning_candidate_deactivated_activate_idx ON lapi_pruning_candidate_deactivated USING btree (activate_event_sequential_id);

create or replace view debug.lapi_pruning_candidate_deactivated as
select
    deactivate_event_sequential_id,
    activate_event_sequential_id
from lapi_pruning_candidate_deactivated;

create table lapi_pruning_contract_candidate (
    internal_contract_id bigint not null
);
create index lapi_pruning_contract_candidate_idx ON lapi_pruning_contract_candidate USING btree (internal_contract_id);

create or replace view debug.lapi_pruning_contract_candidate as
select
    internal_contract_id
from lapi_pruning_contract_candidate;
