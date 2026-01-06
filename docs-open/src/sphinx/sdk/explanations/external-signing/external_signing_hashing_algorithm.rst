..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _external_signing_hashing_algo:

==================================
External Signing Hashing Algorithm
==================================

Introduction
============

This document specifies the encoding algorithm used to produce a deterministic hash of a ``com.daml.ledger.api.v2.interactive.PreparedTransaction``.
The resulting hash is signed by the holder of the external party's private key. The signature
authorizes the ledger changes described by the transaction on behalf of the external party.

The specification can be implemented in any language, but certain encoding patterns are biased due to Canton being implemented in a JVM-based
language and using the Java protobuf library. Those biases are made explicit in the specification.

Protobuf serialization is unsuitable for signing cryptographic hashes because it is not canonical.
We must define a more precise encoding specification that can be re-implemented deterministically across languages
and provide the required cryptographic guarantees. See `<https://protobuf.dev/programming-guides/serialization-not-canonical/>`_ for more information on the topic.

Versioning
==========

Hashing Scheme Version
----------------------

The hashing algorithm as a whole is versioned. This enables updates to accommodate changes in the underlying Daml format,
or, for instance, to the way the protocol verifies signatures. The implementation must respect the specification of the version it implements.

.. literalinclude:: CANTON/community/ledger-api-proto/src/main/protobuf/com/daml/ledger/api/v2/interactive/interactive_submission_service.proto
    :start-after: [docs-entry-start: HashingSchemeVersion]
    :end-before: [docs-entry-end: HashingSchemeVersion]
    :caption: Hashing Scheme Versions

The hashing algorithm is tied to the protocol version of the synchronizer used to synchronize the transaction.
Specifically, each hashing scheme version is supported on one or several protocol versions.
Implementations must use a hashing scheme version supported on the synchronizer on which the transaction is submitted.

==================  =========================
Protocol Version    Supported Hashing Schemes
==================  =========================
v33                 v2
==================  =========================

Transaction Nodes
-----------------

Transaction nodes are additionally individually versioned with a Daml version (also called LF version).
The encoding version is decoupled from the LF version and implementations should only focus on the hashing version.
However, new LF versions may introduce new fields in nodes or new node types. For that reason, the protobuf representation of a node is
versioned to accommodate those future changes. In practice, every new Daml language version results in a new hashing version.

.. literalinclude:: CANTON/community/ledger-api-proto/src/main/protobuf/com/daml/ledger/api/v2/interactive/interactive_submission_service.proto
    :start-after: [docs-entry-start: DamlTransaction.Node]
    :end-before: [docs-entry-end: DamlTransaction.Node]
    :caption: Versioned Daml Transaction Node
    :dedent: 4

V2
==

General approach
----------------

The hash of the ``PreparedTransaction`` is computed by encoding every protobuf field of the messages to byte arrays,
and feeding those encoded values into a ``SHA-256`` hash builder. The rest of this section details how to deterministically encode
every proto message into a byte array. Sometimes during the process, partially encoded results are hashed with SHA-256, and the resulting hash value serves as the encoding in messages further up.
This is explicit when necessary.

Big Endian notation is used for numeric values.
Furthermore, protobuf numeric values are encoded according to their Java type representation.
Refer to the official protobuf documentation for more information about protobuf to Java type mappings: `<https://protobuf.dev/programming-guides/proto3/#scalar>`_
In particular:

.. note::

    In Java, unsigned 32-bit and 64-bit integers are represented using their signed counterparts, with the top bit simply being stored in the sign bit

Additionally, this is the java library used under the hood in Canton to serialize and deserialize protobuf: `<https://github.com/protocolbuffers/protobuf/tree/v3.25.5/java>`_

Changes from V1
---------------

- Addition of an ``interface_id`` field in :ref:`Fetch nodes <fetch_node_encoding>` for support of Daml interfaces.
- Addition of the hashing scheme version in the :ref:`final hash <final_hash_encoding>` to make the hash more robust to cross version collisions.
- Replace ``ledger_effective_time`` in the :ref:`metadata <metadata_encoding>` with ``min_ledger_effective_time`` and ``max_ledger_effective_time``.

    * These effectively replace a fixed ledger time with time bounds, allowing Daml Models to make assertions based on time without restricting the signing window as was required with a fixed set ledger time.

Notation and Utility Functions
------------------------------

- ``encode``: Function that takes a protobuf message or primitive type `T` and transforms it into an array of bytes: `encode: T => byte[]`

e.g:

.. code-block::

    encode(false) = [0x00]

- ``to_utf_8``: Function converting a Java `String` to its UTF-8 encoded version: ``to_utf_8: string => byte[]``

e.g:

.. code-block::

    to_utf_8("hello") = [0x68, 0x65, 0x6c, 0x6c, 0x6f]

- ``len``: Function returning the size of a collection (``array``, ``list`` etc...) as a signed 4 bytes integer: ``len: Col => Int``

e.g:

.. code-block::

    len([4, 2, 8]) = 3

- ``split``: Function converting a Java ``String`` to a list of ``String``, by splitting the input using the provided delimiter: ``split: (string, char) => byte[]``

e.g:

.. code-block::

    split("com.digitalasset.canton", '.') = ["com", "digitalasset", "canton"]

- ``||``:  Symbol representing concatenation of byte arrays

e.g:

.. code-block::

    [0x00] || [0x01] = [0x00, 0x01]

- ``[]``: Empty byte array. Denotes that the value should not be encoded.

- ``from_hex_string``: Function that takes a string in the hexadecimal format as input and decodes it as a byte array: ``from_hex_string: string => byte[]``

e.g:

.. code-block::

    from_hex_string("08020a") = [0x08, 0x02, 0x0a]

- ``int_to_string``: Function that takes an int and converts it to a string : ``int_to_string: int => string``

e.g:

.. code-block::

    int_to_string(42) = "42"

- ``some``: Value wrapped in a defined optional. Should be encoded as a defined optional value: ``some: T => optional T``

e.g:

.. code-block::

    encode(some(5)) = 0x01 || encode(5)

See encoding of optional values below for details.

Primitive Types
---------------

Unless otherwise specified, this is how primitive protobuf types should be encoded.

.. note::

    Not all protobuf types are described here, only the ones necessary to encode a `PreparedTransaction` message.

.. important::

    Even default values must be included in the encoding.
    For instance if an int32 field is not set in the serialized protobuf, its default value (0) should be encoded.
    Similarly, an empty repeated field still results in a `0x00` byte encoding (see the `repeated` section below for more details)

google.protobuf.Empty
^^^^^^^^^^^^^^^^^^^^^

.. code-block::

    fn encode(empty): 0x00

bool
^^^^

.. code-block::

    fn encode(bool):
       if (bool)
          0x01
       else
          0x00

int64 - uint64 - sint64 - sfixed64
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

    fn encode(long):
       long # Java `Long` value equivalent: 8 bytes

e.g:

.. code-block::

    31380 (base 10) == 0x0000000000007a94

int32 - uint32 - sint32 - sfixed32
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block::

    fn encode(int):
       int # Java `Int` value equivalent: 4 bytes

e.g:

.. code-block::

    5 (base 10) == 0x00000005

bytes / byte[]
^^^^^^^^^^^^^^

.. code-block::

    fn encode(bytes):
   encode(len(bytes)) || bytes

e.g

.. code-block::

    0x68656c6c6f ->
        0x00000005 || # length
        0x68656c6c6f # content

string
^^^^^^

.. code-block::

    fn encode(string):
       encode(to_utf8(string))

e.g

.. code-block::

    "hello" ->
        0x00000005 || # length
        0x68656c6c6f # utf-8 encoding of "hello"

Collections / Wrappers
----------------------

repeated
^^^^^^^^

``repeated`` protobuf fields represent an ordered collection of values of a specific message of type `T``.
It is critical that the order of values in the list is not modified, both for the encoding process and in the protobuf
itself when submitting the transaction for execution.
Below is the pseudocode algorithm encoding a protobuf value ``repeated T list;``

.. code-block::

    fn encode(list):
       # prefix the result with the serialized length of the list
       result = encode(len(list)) # (result is mutable)

       # successively add encoded elements to the result, in order
       for each element in list:
          result = result || encode(element)

       return result

.. note::

    This encoding function also applies to lists generated from utility functions (e.g: ``split``).

optional
^^^^^^^^

.. code-block::

    fn encode(optional):
       if (is_set(optional))
          0x01 || encode(optional.value)
       else
          0x00

``is_set`` returns ``true`` if the value was set in the protobuf, ``false`` otherwise.

map
^^^

The ordering of ``map`` entries in protobuf serialization is not guaranteed, making it problematic for deterministic encoding.
To address this, ``repeated`` values are used instead of ``map`` throughout the protobuf definitions.

gRPC Ledger API Value
---------------------

Encoding for the ``Value`` message defined in ``com.daml.ledger.api.v2.value.proto``
For clarity, all value types are exhaustively listed here.
Each value is prefixed by a tag unique to its type, which is explicitly specified for each value below.

Unit
^^^^

.. code-block::

    fn encode(unit):
        0X00 # Unit Type Tag

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L27>`__

Bool
^^^^

.. code-block::

    fn encode(bool):
        0X01 || # Bool Type Tag
    encode(bool) # Primitive boolean encoding

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L30>`__

Int64
^^^^^

.. code-block::

    fn encode(int64):
        0X02 || # Int64 Type Tag
        encode(int64) # Primitive int64 encoding

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L32>`__

Numeric
^^^^^^^

.. code-block::

    fn encode(numeric):
        0X03 || # Numeric Type Tag
        encode(numeric) # Primitive string encoding

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L52>`__

Timestamp
^^^^^^^^^

.. code-block::

    fn encode(timestamp):
        0X04 || # Timestamp Type Tag
        encode(timestamp) # Primitive sfixed64 encoding

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L45>`__

Date
^^^^

.. code-block::

    fn encode(date):
        0X05 || # Date Type Tag
        encode(date) # Primitive int32 encoding

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L37>`__

Party
^^^^^

.. code-block::

    fn encode(party):
        0X06 || # Party Type Tag
        encode(party) # Primitive string encoding

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L56>`__

Text
^^^^

.. code-block::

    fn encode(text):
        0X07 || # Text Type Tag
        encode(text) # Primitive string encoding

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L59>`__

Contract_id
^^^^^^^^^^^

.. code-block::

    fn encode(contract_id):
        0X08 || # Contract Id Type Tag
        from_hex_string(contract_id) # Contract IDs are hexadecimal strings, so they need to be decoded as such. They should not be encoded as classic strings

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L63>`__

Optional
^^^^^^^^

.. code-block::

    fn encode(optional):
       if (optional.value is set)
          0X09 || # Optional Type Tag
          0x01 || # Defined optional
          encode(optional.value)
       else
          0X09 || # Optional Type Tag
          0x00 || # Undefined optional

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L66>`__

Note this is conceptually the same as for the primitive `optional` protobuf modifier, with the addition of the type tag prefix.

List
^^^^

.. code-block::

    fn encode(list):
       0X0a || # List Type Tag
       encode(list.elements)

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L156-L160>`__

TextMap
^^^^^^^

.. code-block::

    fn encode(text_map):
       0X0b || # TextMap Type Tag
       encode(text_map.entries)

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L169-L176>`__

**TextMap.Entry**

.. code-block::

    fn encode(entry):
       encode(entry.key) || encode(entry.value)

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L170C3-L173C4>`__

Record
^^^^^^

.. code-block::

    fn encode(record):
       0X0c || # Record Type Tag
       encode(some(record.record_id)) ||
       encode(record.fields)

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L87-L95>`__

**RecordField**

.. code-block::

    fn encode(record_field):
       encode(some(record_field.label)) || encode(record_field.value)

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L98-L109>`__

Variant
^^^^^^^

.. code-block::

    fn encode(variant):
       0X0d || # Variant Type Tag
       encode(some(variant.variant_id)) ||
       encode(variant.constructor) || encode(variant.value)

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L79>`__

Enum
^^^^

.. code-block::

    fn encode(enum):
       0X0e || # Enum Type Tag
       encode(some(enum.enum_id)) ||
       encode(enum.constructor)

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L82>`__

GenMap
^^^^^^

.. code-block::

    fn encode(gen_map):
       0X0f || # GenMap Type Tag
       encode(gen_map.entries)

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L178-L185>`__

**GenMap.Entry**

.. code-block::

    fn encode(entry):
       encode(entry.key) || encode(entry.value)

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L179-L182>`__

Identifier
^^^^^^^^^^

.. code-block::

    fn encode(identifier):
       encode(identifier.package_id) || encode(split(identifier.module_name, '.')) || encode(split(identifier.entity_name, '.')))

`Protobuf Definition <https://github.com/digital-asset/daml/blob/b5698e2327b83f7d9a5619395cd9b2de21509ab3/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto#L112-L125>`__

Transaction
===========

A transaction is a forest (list of trees). It is represented with a following protobuf message found `here <https://github.com/digital-asset/daml/blob/ba14c4430b8345e7f0f8b20c3feead2b88c90fb8/sdk/canton/community/ledger-api-proto/src/main/protobuf/com/daml/ledger/api/v2/interactive/interactive_submission_service.proto#L283-L315>`_.

The encoding function for a transaction is

.. code-block::

    fn encode(transaction):
       encode(transaction.version) || encode_node_ids(transaction.roots)

``encode_node_ids(node_ids)`` encodes lists in the same way as described before, except the encoding of a ``node_id`` is
NOT done by encoding it as a string, but instead uses the following ``encode(node_id)`` function:

.. code-block::

    fn encode(node_id):
        for node in nodes:
            if node.node_id == node_id:
               return sha_256(encode_node(node))
        fail("Missing node") # All node ids should have a unique node in the nodes list. If a node is missing it should be reported as a bug.

.. important::

    ``encode(node_id)`` effectively finds the corresponding node in the list of nodes and encodes the node.
    The `node_id` is an opaque value only used to reference nodes and is itself never encoded.
    Additionally, each node's encoding is **hashed using the sha_256 hashing algorithm**. This is relevant when encoding root nodes here as well as
    when recursively encoding sub-nodes of ``Exercise`` and ``Rollback`` nodes as seen below.

Node
----

.. note::

    Each node's encoding is prefixed with additional meta-information about the node, this is made explicit in the encoding of each node.

``Exercise`` and ``Rollback`` nodes both have a ``children`` field that references other nodes by their ``NodeId``.

The following ``find_seed: NodeId => optional bytes`` function is used in the encoding:

.. code-block::

    fn find_seed(node_id):
        for node_seed in node_seeds:
            if int_to_string(node_seed.node_id) == node_id
                return some(node_seed.seed)
        return none

    # There's no need to prefix the seed with its length because it has a fixed length. So its encoding is the identity function
    fn encode_seed(seed):
        seed

    # Normal optional encoding, except the seed is encoded with `encode_seed`
    fn encode_optional_seed(optional_seed):
        if (is_some(optional_seed))
          0x01 || encode_seed(optional_seed.get)
       else
          0x00

``some`` represents a set optional field, ``none`` an empty optional field.

Create
^^^^^^

.. code-block::

    fn encode_node(create):
        0x01 || # Node encoding version
        encode(create.lf_version) || # Node LF version
        0x00 || # Create node tag
        encode_optional_seed(find_seed(node.node_id)) ||
        encode(create.contract_id) ||
        encode(create.package_name) ||
        encode(create.template_id) ||
        encode(create.argument) ||
        encode(create.signatories) ||
        encode(create.stakeholders)

Exercise
^^^^^^^^

.. code-block::

    fn encode_node(exercise):
        0x01 || # Node encoding version
        encode(exercise.lf_version) || # Node LF version
        0x01 || # Exercise node tag
        encode_seed(find_seed(node.node_id).get) ||
        encode(exercise.contract_id) ||
        encode(exercise.package_name) ||
        encode(exercise.template_id) ||
        encode(exercise.signatories) ||
        encode(exercise.stakeholders) ||
        encode(exercise.acting_parties) ||
        encode(exercise.interface_id) ||
        encode(exercise.choice_id) ||
        encode(exercise.chosen_value) ||
        encode(exercise.consuming) ||
        encode(exercise.exercise_result) ||
        encode(exercise.choice_observers) ||
        encode(exercise.children)

.. important::

    For Exercise nodes, the node seed **MUST** be defined. Therefore it is encoded as a **non** optional field, as noted via the
    ``.get`` in ``find_seed(node.node_id).get``. If the seed of an exercise node cannot be found in the list of ``node_seeds``, encoding must be stopped
    and it should be reported as a bug.

.. note::

    The last encoded value of the exercise node is its ``children`` field. This recursively traverses the transaction tree.

.. _fetch_node_encoding:

Fetch
^^^^^

.. code-block::

    fn encode_node(fetch):
        0x01 || # Node encoding version
        encode(fetch.lf_version) || # Node LF version
        0x02 || # Fetch node tag
        encode(fetch.contract_id) ||
        encode(fetch.package_name) ||
        encode(fetch.template_id) ||
        encode(fetch.signatories) ||
        encode(fetch.stakeholders) ||
        encode(fetch.interface_id) ||
        encode(fetch.acting_parties)

Rollback
^^^^^^^^

.. code-block::

    fn encode_node(rollback):
       0x01 || # Node encoding version
       0x03 || # Rollback node tag
       encode(rollback.children)

.. note::

    Rollback nodes do not have an lf version.

Transaction Hash
^^^^^^^^^^^^^^^^

Once the transaction is encoded, the hash is obtained by running ``sha_256`` over the encoded byte array, with a hash purpose prefix:

.. code-block::

    fn hash(transaction):
        sha_256(
            0x00000030 || # Hash purpose
            encode(transaction)
        )

.. _metadata_encoding:

Metadata
========

The final part of ``PreparedTransaction`` is metadata.
Note that all fields of the metadata need to be signed. Only some fields contribute to the ledger change triggered by the transaction.
The rest of the fields are required by the Canton protocol but either have no impact on the ledger change,
or have already been signed indirectly by signing the transaction itself.

.. code-block::

    fn encode(metadata, prepare_submission_request):
        0x01 || # Metadata Encoding Version
        encode(metadata.submitter_info.act_as) ||
        encode(metadata.submitter_info.command_id) ||
        encode(metadata.transaction_uuid) ||
        encode(metadata.mediator_group) ||
        encode(metadata.synchronizer_id) ||
        encode(metadata.min_ledger_effective_time) ||
        encode(metadata.max_ledger_effective_time) ||
        encode(metadata.submission_time) ||
        encode(metadata.disclosed_events)

ProcessedDisclosedContract
--------------------------

.. code-block::

    fn encode(processed_disclosed_contract):
        encode(processed_disclosed_contract.created_at) ||
        encode(processed_disclosed_contract.contract)

Metadata Hash
-------------

Once the metadata is encoded, the hash is obtained by running ``sha_256`` over the encoded byte array, with a hash purpose prefix:

.. code-block::

    fn hash(metadata):
        sha_256(
            0x00000030 || # Hash purpose
            encode(metadata)
        )

.. _final_hash_encoding:

Final Hash
==========

Finally, compute the hash that needs to be signed to commit to the ledger changes.

.. code-block::

    fn encode(prepared_transaction):
        0x00000030 || # Hash purpose
        0x02 || # Hashing Scheme Version
        hash(transaction) ||
        hash(metadata)

.. code-block::

    fn hash(prepared_transaction):
        sha_256(encode(prepared_transaction))

This resulting hash must be signed with the protocol signing private key(s) used to onboard the external party.
Both the signature along with the ``PreparedTransaction`` must be sent to the API to submit the transaction to the ledger.

Example
=======

Example implementation in Python

.. toggle::

    .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/daml_transaction_hashing_v2.py
