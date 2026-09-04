-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Drop the view
drop view debug.lapi_events_party_to_participant;

-- Add a party string field to the party-to-participant events table.
alter table lapi_events_party_to_participant
    add column party varchar collate "C";

-- Populate the party field using the mapping between external_string and internal_id
-- available in the lapi_string_interning table.
update lapi_events_party_to_participant e
-- The interned string is prefixed with "p|", which is stripped here.
set party = substring(si.external_string from 3)
    from lapi_string_interning si
where si.internal_id = e.party_id
  and si.external_string is not null;

create or replace view debug.lapi_events_party_to_participant as
select
    event_sequential_id,
    event_offset,
    lower(encode(update_id, 'hex')) as update_id,
    party_id as party_id,
    party as party,
    participant_id,
    participant_permission,
    participant_authorization_event,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    debug.canton_timestamp(record_time) as record_time,
    lower(encode(trace_context, 'hex')) as trace_context
from lapi_events_party_to_participant;

-- external_string in the string interning table may never be null, that's a system invariant, so we enforce it in the schema.
alter table lapi_string_interning
    alter column external_string set not null;

-- Likewise, party in the party-to-participant events table may never be null, so we enforce it in the schema.
alter table lapi_events_party_to_participant
    alter column party set not null;
