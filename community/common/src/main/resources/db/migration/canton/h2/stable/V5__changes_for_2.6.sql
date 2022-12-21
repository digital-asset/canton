-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE pruning_schedules(
  -- node_type is one of "PAR", "MED", or "SEQ"
  -- since mediator and sequencer sometimes share the same db
  node_type varchar(3) not null primary key,
  cron varchar(300) not null,
  max_duration bigint not null, -- positive number of seconds
  retention bigint not null -- positive number of seconds
);