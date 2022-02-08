-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table response_aggregations (
  -- identified by the sequencer timestamp (UTC timestamp in microseconds relative to EPOCH)
  request_id bigint not null primary key,
  mediator_request binary large object not null,
  -- UTC timestamp is stored in microseconds relative to EPOCH
  version bigint not null,
  verdict binary large object not null,
  request_trace_context binary large object not null
);