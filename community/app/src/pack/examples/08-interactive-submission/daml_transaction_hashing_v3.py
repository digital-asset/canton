# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Implements V3 of the transaction hashing specification.
# V3 removes node/metadata encoding version prefixes, adds key_opt to Create/Exercise/Fetch,
# adds by_key to Exercise/Fetch, introduces QueryByKey node type, and adds max_record_time to metadata.

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
HASHING_SCHEME_VERSION_V3 = (
    interactive_submission_service_pb2.HashingSchemeVersion.HASHING_SCHEME_VERSION_V3
)
# Byte version for the encoding (\x03)
HASHING_SCHEME_VERSION = HASHING_SCHEME_VERSION_V3.to_bytes(
    length=1, byteorder="big", signed=False
)


def encode_key_with_maintainers(key_with_maintainers):
    """Encodes a GlobalKeyWithMaintainers composite value."""
    return (
        encode_string(key_with_maintainers.key.package_name)
        + encode_identifier(key_with_maintainers.key.template_id)
        + encode_value(key_with_maintainers.key.key)
        + encode_hash(key_with_maintainers.key.hash)
        + encode_repeated(key_with_maintainers.maintainers, encode_string)
    )


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
    elif node.HasField("query_by_key"):
        return encode_query_by_key_node(node.query_by_key)
    raise ValueError("Unsupported node type")


def encode_create_node(
    create,
    node_id,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    return (
        encode_string(create.lf_version)
        + b"\x00"  # Create node tag
        + encode_optional(find_seed(node_id, node_seeds), encode_hash)
        + encode_hex_string(create.contract_id)
        + encode_string(create.package_name)
        + encode_identifier(create.template_id)
        + encode_value(create.argument)
        + encode_repeated(create.signatories, encode_string)
        + encode_repeated(create.stakeholders, encode_string)
        + encode_proto_optional(create, "key", create.key, encode_key_with_maintainers)
    )


def encode_exercise_node(
    exercise,
    node_id,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    return (
        encode_string(exercise.lf_version)
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
        + encode_bool(exercise.by_key)
        + encode_proto_optional(exercise, "key", exercise.key, encode_key_with_maintainers)
        + encode_repeated(exercise.children, encode_node_id(nodes_dict, node_seeds, encode_node))
    )


def encode_fetch_node(fetch, node_id):
    return (
        encode_string(fetch.lf_version)
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
        + encode_bool(fetch.by_key)
        + encode_proto_optional(fetch, "key", fetch.key, encode_key_with_maintainers)
    )


def encode_rollback_node(
    rollback,
    node_id,
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
):
    return (
        b"\x03"  # Rollback node tag
        + encode_repeated(rollback.children, encode_node_id(nodes_dict, node_seeds, encode_node))
    )


def encode_query_by_key_node(query_by_key):
    return (
        encode_string(query_by_key.lf_version)
        + b"\x04"  # QueryByKey node tag
        + encode_string(query_by_key.package_name)
        + encode_identifier(query_by_key.template_id)
        + encode_bool(query_by_key.exhaustive)
        + encode_key_with_maintainers(query_by_key.key)
        + encode_repeated(query_by_key.result, encode_hex_string)
    )


def hash_metadata(metadata):
    encoded_metadata = encode_metadata(metadata)
    return sha256(PREPARED_TRANSACTION_HASH_PURPOSE + encoded_metadata)


def encode_metadata(metadata):
    return (
        encode_repeated(metadata.submitter_info.act_as, encode_string)
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
        + encode_proto_optional(metadata, "max_record_time", metadata.max_record_time, encode_int64)
    )


def encode_disclosed_contract(contract):
    return encode_int64(contract.created_at) + sha256(
        encode_create_node(contract.contract, "unused_node_id", [])
    )


def encode_input_contract(contract):
    return encode_int64(contract.created_at) + sha256(
        encode_create_node(contract.v1, "unused_node_id", [])
    )

