# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Common encoding utilities shared between hashing scheme versions V2 and V3.

import com.daml.ledger.api.v2.interactive.interactive_submission_service_pb2 as interactive_submission_service_pb2
import hashlib
import struct

# Hash purpose reserved for prepared transaction
PREPARED_TRANSACTION_HASH_PURPOSE = b"\x00\x00\x00\x30"


def encode_bool(value):
    return b"\x01" if value else b"\x00"


def encode_int32(value):
    if not (-(2**31) <= value < 2**31):
        raise ValueError(f"Value {value} out of range for int32")
    return struct.pack(">i", value)


def encode_int64(value):
    return struct.pack(">q", value)


def encode_string(value):
    utf8_bytes = value.encode("utf-8")
    return encode_bytes(utf8_bytes)


def encode_bytes(value):
    length = encode_int32(len(value))
    return length + value


# Like encode_bytes but without the length prefix, as hashes have a fixed size
def encode_hash(value):
    return value


def encode_hex_string(value):
    return encode_bytes(bytes.fromhex(value))


def encode_optional(value, encode_fn):
    if value is not None:
        return b"\x01" + encode_fn(value)
    else:
        return b"\x00"


def encode_proto_optional(parent_value, field_name, value, encode_fn):
    if parent_value.HasField(field_name):
        return b"\x01" + encode_fn(value)
    else:
        return b"\x00"


def encode_repeated(values, encode_fn):
    length = encode_int32(len(values))
    encoded_values = b"".join(encode_fn(v) for v in values)
    return length + encoded_values


def sha256(data):
    return hashlib.sha256(data).digest()


def find_seed(
    node_id, node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed]
):
    for node_seed in node_seeds:
        if str(node_seed.node_id) == node_id:
            return node_seed.seed
    return None


def encode_identifier(identifier):
    return (
        encode_string(identifier.package_id)
        + encode_repeated(identifier.module_name.split("."), encode_string)
        + encode_repeated(identifier.entity_name.split("."), encode_string)
    )


def encode_node_id(
    nodes_dict: dict,
    node_seeds: [interactive_submission_service_pb2.DamlTransaction.NodeSeed],
    encode_node_fn,
):
    def encode(node_id):
        node = nodes_dict[node_id]
        return sha256(encode_node_fn(node, nodes_dict, node_seeds))

    return encode


def encode_transaction(transaction, nodes_dict, node_seeds, encode_node_fn):
    version = encode_string(transaction.version)
    roots = encode_repeated(
        transaction.roots, encode_node_id(nodes_dict, node_seeds, encode_node_fn)
    )
    return version + roots


def encode_value(value):
    if value.HasField("unit"):
        return b"\x00"
    elif value.HasField("bool"):
        return b"\x01" + encode_bool(value.bool)
    elif value.HasField("int64"):
        return b"\x02" + encode_int64(value.int64)
    elif value.HasField("numeric"):
        return b"\x03" + encode_string(value.numeric)
    elif value.HasField("timestamp"):
        return b"\x04" + encode_int64(value.timestamp)
    elif value.HasField("date"):
        return b"\x05" + encode_int32(value.date)
    elif value.HasField("party"):
        return b"\x06" + encode_string(value.party)
    elif value.HasField("text"):
        return b"\x07" + encode_string(value.text)
    elif value.HasField("contract_id"):
        return b"\x08" + encode_hex_string(value.contract_id)
    elif value.HasField("optional"):
        return b"\x09" + encode_proto_optional(
            value.optional, "value", value.optional.value, encode_value
        )
    elif value.HasField("list"):
        return b"\x0a" + encode_repeated(value.list.elements, encode_value)
    elif value.HasField("text_map"):
        return b"\x0b" + encode_repeated(value.text_map.entries, encode_text_map_entry)
    elif value.HasField("record"):
        return (
            b"\x0c"
            + encode_proto_optional(
                value.record, "record_id", value.record.record_id, encode_identifier
            )
            + encode_repeated(value.record.fields, encode_record_field)
        )
    elif value.HasField("variant"):
        return (
            b"\x0d"
            + encode_proto_optional(
                value.variant, "variant_id", value.variant.variant_id, encode_identifier
            )
            + encode_string(value.variant.constructor)
            + encode_value(value.variant.value)
        )
    elif value.HasField("enum"):
        return (
            b"\x0e"
            + encode_proto_optional(
                value.enum, "enum_id", value.enum.enum_id, encode_identifier
            )
            + encode_string(value.enum.constructor)
        )
    elif value.HasField("gen_map"):
        return b"\x0f" + encode_repeated(value.gen_map.entries, encode_gen_map_entry)
    raise ValueError("Unsupported value type")


def encode_text_map_entry(entry):
    return encode_string(entry.key) + encode_value(entry.value)


def encode_record_field(field):
    return encode_optional(field.label, encode_string) + encode_value(field.value)


def encode_gen_map_entry(entry):
    return encode_value(entry.key) + encode_value(entry.value)


def create_nodes_dict(prepared_transaction):
    nodes_dict = {}
    for node in prepared_transaction.transaction.nodes:
        nodes_dict[node.node_id] = node
    return nodes_dict

