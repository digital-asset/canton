-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- Events: Generic Topology Events
---------------------------------------------------------------------------------------------------
create table lapi_events_generic_topology_events (
    event_sequential_id bigint not null,
    event_offset bigint not null,
    update_id bytea not null,
    synchronizer_id integer not null,
    record_time bigint not null,
    event_type smallint not null,
    payload bytea not null,
    trace_context bytea not null
);

create index lapi_events_generic_topology_events_event_seq_id_idx on lapi_events_generic_topology_events using btree (event_sequential_id);

create index lapi_events_generic_topology_events_sid_recordt_idx on lapi_events_generic_topology_events using btree (synchronizer_id, record_time);

create or replace function debug.lapi_event_type(smallint) returns varchar as -- collate "C" result. This comment is necessary to satisfy our linter
$$
select
  case
    when $1 = 1 then 'Activate-Create'
    when $1 = 2 then 'Activate-Assign'
    when $1 = 3 then 'Deactivate-Consuming-Exercise'
    when $1 = 4 then 'Deactivate-Unassign'
    when $1 = 5 then 'Witnessed-Non-Consuming-Exercise'
    when $1 = 6 then 'Witnessed-Create'
    when $1 = 7 then 'Witnessed-Consuming-Exercise'
    when $1 = 8 then 'Topology-PartyToParticipant'
    when $1 = 9 then 'Topology-DynamicSynchronizerParameters'
    when $1 is null then 'None'
    else $1::text
  end;
$$
  language sql
  immutable
  called on null input;

create or replace view debug.lapi_events_generic_topology_events as
  select
    event_sequential_id,
    event_offset,
    lower(encode(update_id, 'hex')) as update_id,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    debug.canton_timestamp(record_time) as record_time,
    debug.lapi_event_type(event_type) as event_type,
    lower(encode(payload, 'hex')) as payload,
    lower(encode(trace_context, 'hex')) as trace_context
  from lapi_events_generic_topology_events;

