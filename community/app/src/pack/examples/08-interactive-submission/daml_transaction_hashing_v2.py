# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Implements V2 of the transaction hashing specification defined in the README.md at https://github.com/digital-asset/canton/blob/main/community/ledger-api/src/release-line-3.2/protobuf/com/daml/ledger/api/v2/interactive/README.md

import com.daml.ledger.api.v2.interactive.interactive_submission_service_pb2 as interactive_submission_service_pb2
from daml_transaction_hashing_common import (
    PREPARED_TRANSACTION_HASH_PURPOSE,
    encode_bool,
    encode_hex_string,
    encode_identifier,
    encode_int32,
    encode_int64,
    encode_node_id,
    encode_optional,
    encode_proto_optional,
    encode_repeated,
    encode_string,
    encode_hash,
    encode_value,
    encode_transaction,
    find_seed,
    sha256,
    create_nodes_dict,
)

# Version of the hashing scheme implemented in this file as a byte
HASHING_SCHEME_VERSION_V2 = (
    interactive_submission_service_pb2.HashingSchemeVersion.HASHING_SCHEME_VERSION_V2
)
# Byte version for the encoding (\x02)
HASHING_SCHEME_VERSION = HASHING_SCHEME_VERSION_V2.to_bytes(
    length=1, byteorder="big", signed=False
)
# Version of the protobuf encoding the transaction nodes
NODE_ENCODING_VERSION = b"\x01"


def encode_prepared_transaction(
    prepared_transaction: interactive_submission_service_pb2.PreparedTransaction,
    nodes_dict: dict,
):
    transaction_hash = hash_transaction(prepared_transaction.transaction, nodes_dict)
    metadata_hash = hash_metadata(prepared_transaction.metadata)
    return sha256(
        PREPARED_TRANSACTION_HASH_PURPOSE
        + HASHING_SCHEME_VERSION
        + transaction_hash
        + metadata_hash
    )


def hash_transaction(
    transaction: interactive_submission_service_pb2.DamlTransaction, nodes_dict: dict
):
    encoded_transaction = encode_transaction(
        transaction, nodes_dict, transaction.node_seeds, encode_node
    )
    return sha256(PREPARED_TRANSACTION_HASH_PURPOSE + encoded_transaction)


def encode_node(
    node,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    node_id = node.node_id
    if node.HasField("v1"):
        return encode_node_v1(node.v1, node_id, nodes_dict, node_seeds)
    raise ValueError("Unsupported node version")


def encode_node_v1(
    node,
    node_id,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    if node.HasField("create"):
        return encode_create_node(node.create, node_id, node_seeds)
    elif node.HasField("exercise"):
        return encode_exercise_node(node.exercise, node_id, nodes_dict, node_seeds)
    elif node.HasField("fetch"):
        return encode_fetch_node(node.fetch, node_id)
    elif node.HasField("rollback"):
        return encode_rollback_node(node.rollback, node_id, nodes_dict, node_seeds)
    raise ValueError("Unsupported node type")


def encode_create_node(
    create,
    node_id,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    return (
        NODE_ENCODING_VERSION
        + encode_string(create.lf_version)
        + b"\x00"  # Create node tag
        + encode_optional(find_seed(node_id, node_seeds), encode_hash)
        + encode_hex_string(create.contract_id)
        + encode_string(create.package_name)
        + encode_identifier(create.template_id)
        + encode_value(create.argument)
        + encode_repeated(create.signatories, encode_string)
        + encode_repeated(create.stakeholders, encode_string)
    )


def encode_exercise_node(
    exercise,
    node_id,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    return (
        NODE_ENCODING_VERSION
        + encode_string(exercise.lf_version)
        + b"\x01"  # Exercise node tag
        + encode_hash(find_seed(node_id, node_seeds))
        + encode_hex_string(exercise.contract_id)
        + encode_string(exercise.package_name)
        + encode_identifier(exercise.template_id)
        + encode_repeated(exercise.signatories, encode_string)
        + encode_repeated(exercise.stakeholders, encode_string)
        + encode_repeated(exercise.acting_parties, encode_string)
        + encode_proto_optional(
            exercise, "interface_id", exercise.interface_id, encode_identifier
        )
        + encode_string(exercise.choice_id)
        + encode_value(exercise.chosen_value)
        + encode_bool(exercise.consuming)
        + encode_proto_optional(
            exercise, "exercise_result", exercise.exercise_result, encode_value
        )
        + encode_repeated(exercise.choice_observers, encode_string)
        + encode_repeated(exercise.children, encode_node_id(nodes_dict, node_seeds, encode_node))
    )


def encode_fetch_node(fetch, node_id):
    return (
        NODE_ENCODING_VERSION
        + encode_string(fetch.lf_version)
        + b"\x02"  # Fetch node tag
        + encode_hex_string(fetch.contract_id)
        + encode_string(fetch.package_name)
        + encode_identifier(fetch.template_id)
        + encode_repeated(fetch.signatories, encode_string)
        + encode_repeated(fetch.stakeholders, encode_string)
        + encode_proto_optional(
            fetch, "interface_id", fetch.interface_id, encode_identifier
        )
        + encode_repeated(fetch.acting_parties, encode_string)
    )


def encode_rollback_node(
    rollback,
    node_id,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    return (
        NODE_ENCODING_VERSION
        + b"\x03"  # Rollback node tag
        + encode_repeated(rollback.children, encode_node_id(nodes_dict, node_seeds, encode_node))
    )


def hash_metadata(metadata):
    encoded_metadata = encode_metadata(metadata)
    return sha256(PREPARED_TRANSACTION_HASH_PURPOSE + encoded_metadata)


def encode_metadata(metadata):
    return (
        b"\x01"  # Metadata encoding version
        + encode_repeated(metadata.submitter_info.act_as, encode_string)
        + encode_string(metadata.submitter_info.command_id)
        + encode_string(metadata.transaction_uuid)
        + encode_int32(metadata.mediator_group)
        + encode_string(metadata.synchronizer_id)
        + encode_proto_optional(
            metadata,
            "min_ledger_effective_time",
            metadata.min_ledger_effective_time,
            encode_int64,
        )
        + encode_proto_optional(
            metadata,
            "max_ledger_effective_time",
            metadata.max_ledger_effective_time,
            encode_int64,
        )
        + encode_int64(metadata.preparation_time)
        + encode_repeated(metadata.input_contracts, encode_input_contract)
    )


def encode_input_contract(contract):
    return encode_int64(contract.created_at) + sha256(
        encode_create_node(contract.v1, "unused_node_id", [])
    )


