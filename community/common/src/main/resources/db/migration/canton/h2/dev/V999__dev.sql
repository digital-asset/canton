-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- This is a dummy column we are adding in order to test that adding dev version migrations
-- works properly. DO NOT MOVE THIS TO STABLE
ALTER TABLE node_id ADD COLUMN test_column INT NOT NULL DEFAULT 0;

-- TODO(#12373) Move this to stable when releasing BFT: BEGIN
CREATE TABLE in_flight_aggregation(
    aggregation_id varchar(300) not null primary key,
    -- UTC timestamp in microseconds relative to EPOCH
    max_sequencing_time bigint not null,
    -- serialized aggregation rule,
    aggregation_rule binary large object not null
);

CREATE INDEX in_flight_aggregation_max_sequencing_time on in_flight_aggregation(max_sequencing_time);

CREATE TABLE in_flight_aggregated_sender(
    aggregation_id varchar(300) not null,
    sender varchar(300) not null,
    -- UTC timestamp in microseconds relative to EPOCH
    sequencing_timestamp bigint not null,
    signatures binary large object not null,
    primary key (aggregation_id, sender),
    constraint foreign_key_in_flight_aggregated_sender foreign key (aggregation_id) references in_flight_aggregation(aggregation_id) on delete cascade
);

-- stores the topology-x state transactions
CREATE TABLE topology_transactions_x (
    -- serial identifier used to preserve insertion order
    id bigserial not null primary key,
    -- the id of the store
    store_id varchar(300) not null,
    -- the timestamp at which the transaction is sequenced by the sequencer
    -- UTC timestamp in microseconds relative to EPOCH
    sequenced bigint not null,
    -- type of transaction (refer to TopologyMappingX.Code)
    transaction_type int not null,
    -- the namespace this transaction is operating on
    namespace varchar(300) not null,
    -- the optional identifier this transaction is operating on (yields a uid together with namespace)
    -- a null value is represented as "", as null is never equal in indexes for postgres, which would
    -- break the unique index
    identifier varchar(300) not null,
    -- The topology mapping key hash, to uniquify and aid efficient lookups.
    -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
    mapping_key_hash varchar(300) not null,
    -- the serial_counter describes the change order within transactions of the same mapping_key_hash
    -- (redundant also embedded in instance)
    serial_counter int not null,
    -- validity window, UTC timestamp in microseconds relative to EPOCH
    -- so `TopologyChangeOpX.Replace` transactions have an effect for valid_from < t <= valid_until
    -- a `TopologyChangeOpX.Remove` will have valid_from = valid_until
    valid_from bigint not null,
    valid_until bigint null,
    -- operation
    -- 1: Remove
    -- 2: Replace (upsert/merge semantics)
    operation int not null,
    -- The raw transaction, serialized using the proto serializer.
    instance binary large object not null,
    -- The transaction hash, to uniquify and aid efficient lookups.
    -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
    tx_hash varchar(300) not null,
    -- flag / reason why this transaction is being rejected
    -- therefore: if this field is NULL, then the transaction is included. if it is non-null shows the reason why it is invalid
    rejection_reason varchar(300) null,
    -- is_proposal indicates whether the transaction still needs BFT-consensus
    -- false if and only if the transaction is accepted
    -- (redundant also embedded in instance)
    is_proposal boolean not null,
    representative_protocol_version integer not null,
    -- index used for idempotency during crash recovery
    unique (store_id, mapping_key_hash, serial_counter, valid_from, operation, representative_protocol_version)
);
CREATE INDEX topology_transactions_x_idx ON topology_transactions_x (store_id, transaction_type, namespace, identifier, valid_until, valid_from);
-- TODO(#11255): Decide whether we want additional indices by mapping_key_hash and tx_hash (e.g. for update/removal and lookups)
-- TODO(#11255): Come up with columns/indexing for efficient ParticipantId => Seq[PartyId] lookup

-- TODO(#11255): Remove table that persists the sequencer_id to ensure that the mediator can always initialize itself with its sequencer
CREATE TABLE sequencer_info_for_mediator_x (
  -- this lock column ensures that there can only ever be a single row: https://stackoverflow.com/questions/3967372/sql-server-how-to-constrain-a-table-to-contain-a-single-row
  lock char(1) not null default 'X' primary key check (lock = 'X'),
  sequencer_id varchar(300) not null
);

-- TODO(#12373) Move this to stable when releasing BFT: END
