-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- H2 does not support partial indexes.
create index sequencer_members_onboarding_idx
    on sequencer_members (status, registered_ts);

-- participant event category to activation/deactivation tables
alter table lapi_events_activate_contract add column participant_event_category int;
alter table lapi_events_deactivate_contract add column participant_event_category int;
