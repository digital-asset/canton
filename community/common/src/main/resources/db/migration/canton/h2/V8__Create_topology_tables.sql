-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- provides a serial enumeration of static strings so we don't store the same string over and over in the db
-- currently only storing uids
create table static_strings (
    -- serial identifier of the string (local to this node)
    id serial not null primary key,
    -- the expression
    string varchar(300) not null,
    -- the source (what kind of string are we storing here)
    source int NOT NULL,
    unique(string, source)
);

-- stores the topology state transactions
create table topology_transactions (
    -- serial identifier used to preserve insertion order
    id serial not null primary key,
    -- the id of the store
    store_id varchar(300) not null,
    -- type of transaction (refer to DomainTopologyTransaction companion object)
    transaction_type int not null,
    -- the namespace this transaction is operating on
    namespace varchar(300) not null,
    -- the optional identifier this transaction is operating on (yields a uid together with namespace)
    -- a null value is represented as "", as null is never equal in indexes for postgres, which would
    -- break the unique index
    identifier varchar(300) not null,
    -- the optional element-id of this transaction (signed topology transactions have one)
    -- same not null logic as for identifier
    element_id varchar(300) not null,
    -- the optional secondary uid (only used by party to participant mappings to compute cascading updates)
    secondary_namespace varchar(300) null,
    secondary_identifier varchar(300) null,
    -- validity window, UTC timestamp in microseconds relative to EPOCH
    -- so Add transactions have an effect for valid_from < t <= valid_until
    -- a remove will have valid_from = valid_until
    valid_from bigint not null,
    valid_until bigint null,
    -- operation
    -- 1: Add
    -- 2: Remove
    operation int not null,
    -- The raw transaction, serialized using the proto serializer.
    instance binary large object not null,
    -- flag / reason why this transaction is being ignored
    -- therefore: if this field is NULL, then the transaction is included. if it is non-null, the reason why it is invalid is included
    ignore_reason varchar null,
    -- index used for idempotency during crash recovery
    unique (store_id, transaction_type, namespace, identifier, element_id, valid_from, operation)
);
CREATE INDEX topology_transactions_idx ON topology_transactions (store_id, transaction_type, namespace, identifier, element_id, valid_until, valid_from);

-- Stores the identity of the node - its assigned member identity and its instance
-- This table should always have at most one entry which is a unique identifier for the member which consists of a string identifier and a fingerprint of a signing key
create table node_id(
  identifier varchar(300) not null,
  namespace varchar(300) not null,
  primary key (identifier, namespace)
);

-- Stores the local party metadata
create table party_metadata (
  -- party id as string
  party_id varchar(300) not null,
  -- the display name which should be exposed via the ledger-api server
  display_name varchar(300) null,
  -- the main participant id of this party which is our participant if the party is on our node (preferred) or the remote participant
  participant_id varchar(300) null,
  -- the submission id used to synchronise the ledger api server
  submission_id varchar(300) null,
  -- notification flag used to keep track about pending synchronisations
  notified boolean not null default false,
  -- the time when this change will be or became effective
  effective_at bigint not null,
  primary key (party_id)
);
create index idx_party_metadata_notified on party_metadata(notified);

-- Stores the dispatching watermarks
create table topology_dispatching (
  -- the target store we are dispatching to (from is always authorized)
  store_id varchar(300) not null primary key,
  -- the dispatching watermark
  watermark_ts bigint not null
);