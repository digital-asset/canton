-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- Events: ACS commitments
---------------------------------------------------------------------------------------------------
create table lapi_events_acs_commitments (
    event_sequential_id bigint not null,
    event_offset bigint not null,
    update_id binary varying not null,
    synchronizer_id integer not null,
    record_time bigint not null,
    payload binary large object not null,
    trace_context binary large object not null
);

create index lapi_events_acs_commitments_event_sequential_id_idx on lapi_events_acs_commitments (event_sequential_id);

create index lapi_events_acs_commitments_sid_seq_id_idx on lapi_events_acs_commitments (synchronizer_id, event_sequential_id);

