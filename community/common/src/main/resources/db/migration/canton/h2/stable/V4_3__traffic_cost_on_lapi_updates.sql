-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Add traffic_cost column to the lapi_events_[de]activate_contract and lapi_events_various_witnessed
-- to make the cost available on updates
alter table lapi_events_activate_contract add column traffic_cost bigint default null;
alter table lapi_events_deactivate_contract add column traffic_cost bigint default null;
alter table lapi_events_various_witnessed add column traffic_cost bigint default null;
