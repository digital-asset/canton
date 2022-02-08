-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create type pruning_phase as enum ('started', 'completed');

-- Maintains the latest timestamp (by domain) for which ACS pruning has started or finished
create table active_contract_pruning (
  domain_id integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  primary key (domain_id)
);

-- Maintains the latest timestamp (by domain) for which ACS commitment pruning has started or finished
create table commitment_pruning (
  domain_id integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  primary key (domain_id)
);

-- Maintains the latest timestamp (by domain) for which contract key journal pruning has started or finished
create table contract_key_pruning (
  domain_id integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  primary key (domain_id)
);

-- Maintains the latest timestamp (by sequencer client) for which the sequenced event store pruning has started or finished
create table sequenced_event_store_pruning (
  client integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  primary key (client)
);
