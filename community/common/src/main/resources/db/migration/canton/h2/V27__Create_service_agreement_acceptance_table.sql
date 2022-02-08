-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table service_agreement_acceptances (
  agreement_id varchar(300) not null,
  participant_id varchar(300) not null,
  -- Signature of the participant
  signature binary large object not null,
  -- Time of acceptance as UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,

  -- We only store the first acceptance of an agreement for a participant
  primary key (agreement_id, participant_id)
);
