-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop table lapi_party_entries;

-- See the Postgres counterpart of this migration for the rationale.
create index lapi_events_party_to_participant_party_idx
    on lapi_events_party_to_participant (party, event_sequential_id desc);

-- See the Postgres counterpart of this migration for the rationale.
create index lapi_events_party_to_participant_party_id_idx
    on lapi_events_party_to_participant (party_id, participant_id, synchronizer_id, event_sequential_id desc);
