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
