-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table command_deduplication (
  -- hash of the change ID (application_id + command_id + act_as) as a hex string
  change_id_hash varchar(300) primary key,

  -- the application ID that requested the change
  application_id varchar(300) not null,
  -- the command ID
  command_id varchar(300) not null,
  -- the act as parties serialized as a Protobuf blob
  act_as binary large object not null,

  -- the highest offset in the MultiDomainEventLog that yields a completion event with a definite answer for the change ID hash
  offset_definite_answer bigint not null,
  -- the publication time of the offset_definite_answer, measured in microseconds relative to EPOCH
  publication_time_definite_answer bigint not null,
  submission_id_definite_answer varchar(300), -- always optional
  trace_context_definite_answer binary large object not null,

  -- the highest offset in the MultiDomainEventLog that yields an accepting completion event for the change ID hash
  -- always at most the offset_definite_answer
  -- null if there have only been rejections since the last pruning
  offset_acceptance bigint,
  -- the publication time of the offset_acceptance, measured in microseconds relative to EPOCH
  publication_time_acceptance bigint,
  submission_id_acceptance varchar(300),
  trace_context_acceptance binary large object
);
create index idx_command_dedup_offset on command_deduplication(offset_definite_answer);

create table command_deduplication_pruning (
  client integer primary key, -- dummy field to enforce at most one row

  -- The highest offset pruning has been started at.
  pruning_offset varchar not null,
  -- An upper bound to publication times of pruned offsets.
  publication_time bigint not null
);
