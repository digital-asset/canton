-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- the last recorded head clean sequencer counter for each domain
create table head_sequencer_counters (
  -- discriminate between different users of the sequencer counter tracker tables
  client integer not null primary key,
  prehead_counter bigint not null, -- sequencer counter before the first unclean sequenced event
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null
);