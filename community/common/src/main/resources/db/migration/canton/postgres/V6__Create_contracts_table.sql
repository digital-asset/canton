-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Stores the immutable contracts, however a creation of a contract can be rolled back.
create table contracts (
    -- As a participant can be connected to multiple domains, the transactions are stored under a domain id.
    domain_id integer not null,
    contract_id varchar(300) not null,
    -- The contract is serialized using the LF contract proto serializer.
    instance bytea not null,
    -- Metadata: signatories, stakeholders, keys
    -- Stored as a Protobuf blob as H2 will only support typed arrays in 1.4.201
    -- TODO(#3256): change when H2 is upgraded
    metadata bytea not null,
    -- The ledger time when the contract was created.
    ledger_create_time varchar(300) not null,
    -- The request counter of the request that created or divulged the contract
    request_counter bigint not null,
    -- The transaction that created the contract; null for divulged contracts
    creating_transaction_id bytea,
    -- We store metadata of the contract instance for inspection
    package_id varchar(300) not null,
    template_id varchar not null,
    primary key (domain_id, contract_id));

-- Index to speedup ContractStore.find
-- domain_id comes first, because there is always a constraint on it.
-- package_id comes before template_id, because queries with package_id and without template_id make more sense than vice versa.
-- contract_id is left out, because a query with domain_id and contract_id can be served with the primary key.
create index idx_contracts_find on contracts(domain_id, package_id, template_id);
