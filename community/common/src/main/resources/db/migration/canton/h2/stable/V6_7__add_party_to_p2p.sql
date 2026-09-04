-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Add a party string field to the party-to-participant events table.
alter table lapi_events_party_to_participant
    add column party varchar;

-- external_string in the string interning table may never be null, that's a system invariant, so we enforce it in the schema.
alter table lapi_string_interning
    alter column external_string set not null;

-- Likewise, party in the party-to-participant events table may never be null, so we enforce it in the schema.
alter table lapi_events_party_to_participant
    alter column party set not null;
