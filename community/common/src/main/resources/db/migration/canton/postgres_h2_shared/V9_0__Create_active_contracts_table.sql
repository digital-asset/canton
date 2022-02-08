-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- change type: activation [create, transfer-in], deactivation [archive, transfer-out]
-- `deactivation` comes before `activation` so that comparisons `(timestamp, change) <= (bound, 'deactivation')`
-- select only deactivations if the timestamp matches the bound.
create type change_type as enum ('deactivation', 'activation');

-- The specific operation type that introduced a contract change.
create type operation_type as enum ('create', 'transfer-in', 'archive', 'transfer-out');

-- Maintains the status of contracts
create table active_contracts (
    -- As a participant can be connected to multiple domains, the active contracts are stored under a domain id.
    domain_id int not null,
    contract_id varchar(300) not null,
    change change_type not null,
    operation operation_type not null,
    -- UTC timestamp of the time of change in microsecond precision relative to EPOCH
    ts bigint not null,
    -- Request counter of the time of change
    request_counter bigint not null,
    -- optional remote domain id in case of transfers
    remote_domain_id int,
    primary key (domain_id, contract_id, ts, request_counter, change)
);

CREATE index active_contracts_dirty_request_reset_idx ON active_contracts (domain_id, request_counter);

CREATE index active_contracts_contract_id_idx ON active_contracts (contract_id);

CREATE index active_contracts_ts_domain_id_idx ON active_contracts (ts, domain_id);


