-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- This is a dummy column we are adding in order to test that adding dev version migrations
-- works properly. DO NOT MOVE THIS TO STABLE
alter table common_node_id add column test_column int not null default 0;

-- Maintains the status of contracts to be sent to the indexer on behalf of Online Party Replication
create table par_party_replication_indexing (
  synchronizer_idx integer not null,
  -- party id as string
  party_id varchar not null,
  -- UTC timestamp of the time of change in microsecond precision relative to EPOCH
  ts bigint not null,
  -- Repair counter of the time of change lexicographically relative to ts
  repair_counter bigint not null,
  contract_id binary varying not null,

  change change_type not null,
  reassignment_counter bigint not null,

  -- Once the ACS change has been queued to the indexer, holds the assigned batch counter
  indexing_batch_counter bigint null,
  -- Once the ACS change has been queued to the indexer, holds the assigned update_id
  update_id binary large object null,

  is_indexed boolean not null,

  primary key (synchronizer_idx, party_id, ts, repair_counter, contract_id)
);
