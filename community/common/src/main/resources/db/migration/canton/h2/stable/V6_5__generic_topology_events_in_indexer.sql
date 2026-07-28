-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- Events: Generic Topology Events
---------------------------------------------------------------------------------------------------
create table lapi_events_generic_topology_events (
    event_sequential_id bigint not null,
    event_offset bigint not null,
    update_id binary varying not null,
    synchronizer_id integer not null,
    record_time bigint not null,
    event_type smallint not null,
    payload binary large object not null,
    trace_context binary large object not null
);

create index lapi_events_generic_topology_events_event_seq_id_idx on lapi_events_generic_topology_events (event_sequential_id);

create index lapi_events_generic_topology_events_sid_recordt_idx on lapi_events_generic_topology_events (synchronizer_id, record_time);

