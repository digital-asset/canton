-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

drop table lapi_party_entries cascade;

-- The party queries (knownParties/parties) use this index to efficiently retrieve the list of candidate parties.
create index lapi_events_party_to_participant_party_idx
    on lapi_events_party_to_participant
    using btree (party, event_sequential_id desc)
    include (party_id);

-- The party queries (knownParties/parties) use this index to refine the result set in the inner queries.
create index lapi_events_party_to_participant_party_id_idx
    on lapi_events_party_to_participant
    using btree (party_id, participant_id, synchronizer_id, event_sequential_id desc)
    include (participant_authorization_event);
