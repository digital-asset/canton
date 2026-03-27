-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- This is a dummy column we are adding in order to test that adding dev version migrations
-- works properly. DO NOT MOVE THIS TO STABLE
alter table common_node_id add column test_column int not null default 0;

-- Maintains the status of contracts to be sent to the indexer on behalf of Online Party Replication
create table par_party_replication_indexing (
  synchronizer_idx integer not null,
  -- party id as string
  party_id varchar collate "C" not null,
  -- UTC timestamp of the time of change in microsecond precision relative to EPOCH
  ts bigint not null,
  -- Repair counter of the time of change lexicographically relative to ts
  repair_counter bigint not null,
  contract_id bytea not null,

  change change_type not null,
  reassignment_counter bigint not null,

  -- Once the ACS change has been queued to the indexer, holds the assigned batch counter
  indexing_batch_counter bigint null,
  -- Once the ACS change has been queued to the indexer, holds the assigned update_id
  update_id bytea null,

  is_indexed boolean not null,

  primary key (synchronizer_idx, party_id, ts, repair_counter, contract_id)
);

create or replace view debug.par_party_replication_indexing as
  select
    debug.resolve_common_static_string(synchronizer_idx) as synchronizer_idx,
    party_id,
    debug.canton_timestamp(ts) as ts,
    repair_counter,
    lower(encode(contract_id, 'hex')) as contract_id,
    change,
    reassignment_counter,
    indexing_batch_counter,
    lower(encode(update_id, 'hex')) as update_id,
    is_indexed
  from par_party_replication_indexing;
