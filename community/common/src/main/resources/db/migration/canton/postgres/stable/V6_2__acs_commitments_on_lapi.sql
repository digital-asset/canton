-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- Events: ACS commitments
---------------------------------------------------------------------------------------------------
create table lapi_events_acs_commitments (
    event_sequential_id bigint not null,
    event_offset bigint not null,
    update_id bytea not null,
    synchronizer_id integer not null,
    record_time bigint not null,
    payload bytea not null,
    trace_context bytea not null
);

create index lapi_events_acs_commitments_event_sequential_id_idx on lapi_events_acs_commitments using btree (event_sequential_id);

create index lapi_events_acs_commitments_sid_seq_id_idx on lapi_events_acs_commitments using btree (synchronizer_id, event_sequential_id);

create or replace view debug.lapi_events_acs_commitments as
  select
    event_sequential_id,
    event_offset,
    lower(encode(update_id, 'hex')) as update_id,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    debug.canton_timestamp(record_time) as record_time,
    lower(encode(payload, 'hex')) as payload,
    lower(encode(trace_context, 'hex')) as trace_context
  from lapi_events_acs_commitments;

