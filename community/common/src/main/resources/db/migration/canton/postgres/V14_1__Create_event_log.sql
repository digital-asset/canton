-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- create a postgres partial index on associated_domain in the participant_event_log to expedite pruning
create index idx_event_log_associated_domain on event_log (log_id, associated_domain, ts)
  where log_id = 0 -- must be the same as the ParticipantEventLog.ProductionParticipantEventLogId.index
    and associated_domain is not null;
