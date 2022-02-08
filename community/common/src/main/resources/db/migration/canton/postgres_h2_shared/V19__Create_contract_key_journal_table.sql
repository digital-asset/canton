-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- 'unassigned' comes before 'assigned' so that comparisons `(timestamp, change) <= (bound, 'unassigned')`
-- -- select only unassignments if the timestamp matches the bound.
create type key_status as enum ('unassigned', 'assigned');

-- Maintains the number of active contracts for a given key (by hash) as a journal
create table contract_key_journal (
    -- For uniformity with other tables, we store the domain ID with the allocation count,
    -- even though the contract key journal is currently used only for domains with unique contract key (UCK) semantics
    -- and participants can connect only to one UCK domain.
    domain_id int not null,
    contract_key_hash varchar(300) not null,
    status key_status not null,
    -- Timestamp of the time of change in microsecond precision relative to EPOCH
    ts bigint not null,
    -- Request counter of the time of change
    request_counter bigint not null,
    primary key (domain_id, contract_key_hash, ts, request_counter)
);

CREATE index contract_key_journal_dirty_request_reset_idx ON contract_key_journal (domain_id, ts, request_counter);