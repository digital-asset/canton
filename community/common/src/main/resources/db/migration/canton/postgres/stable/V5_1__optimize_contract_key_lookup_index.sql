-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Recreate the activate contract key index with internal_contract_id as an included column.
drop index lapi_events_activate_contract_key_idx;
create index lapi_events_activate_contract_key_idx on lapi_events_activate_contract using btree (create_key_hash, event_sequential_id) include (internal_contract_id) where create_key_hash is not null;
