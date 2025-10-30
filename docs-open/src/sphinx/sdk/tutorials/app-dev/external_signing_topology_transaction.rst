..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _externally_signed_topology_transaction:

=======================================
Externally Signed Topology Transactions
=======================================

Canton's `Topology <identity-manager-1>`_ formalizes a shared state on a synchronizer and provides a secure, distributed mechanism for modifying this state.

This tutorial demonstrates how to build, sign, and submit topology transactions.
It is particularly useful for cases where the signature is provided by a key held externally to the network,
such as in the case of external party onboarding, or initialization of the root namespace of a participant.
This tutorial goes through the steps of importing a root namespace delegation, but can be generalized to any topology mapping.
A root namespace delegation is essentially equivalent to a X509v3 CA root certificate in Canton and creates an associated namespace.

.. important::

    This tutorial is for demo purposes.
    The code snippets should not be used directly in a production environment.

Prerequisites
=============

For simplicity, this tutorial assumes a minimal Canton setup consisting of one participant node connected to one synchronizer (which includes both a sequencer node and a mediator node).

Start Canton
------------

To obtain a Canton artifact refer to the :externalref:`getting started <canton-getting-started>` section.
From the artifact directory, start Canton using the command:

.. code-block::

  ./bin/canton -c examples/01-simple-topology/simple-topology.conf --bootstrap examples/01-simple-topology/simple-ping.canton

Once the "Welcome to Canton" message appears, you are ready to proceed.

.. _tutorial_topology_transaction_setup:

Setup
-----

Navigate to the interactive submission example folder located at ``examples/08-interactive-submission`` in the Canton release artifact.

.. tip::

    The code examples in this tutorial are extracted from scripts located in that folder.

To proceed, gather the following information by running the commands below in the Canton console:

- Admin API endpoint
- Synchronizer ID

.. snippet:: external_signing_topology_transaction
    .. hidden:: bootstrap.synchronizer(
          synchronizerName = "da",
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1, mediator1),
          synchronizerThreshold = 2,
          staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
        )
    .. success:: participant1.config.adminApi.address
    .. success:: participant1.config.adminApi.port.unwrap
    .. success:: sequencer1.synchronizer_id.toProtoPrimitive

In the rest of the tutorial we use the following values, but make sure to replace them with your own:

- Admin API endpoint: ``localhost:4002``
- Synchronizer ID: ``da::12207a94aca813c822c6ae10a1b5478c2ba1077447b468cc66dbd255f60f8fa333e1``

.. _canton_external_topology_manager_write_service:

API
---

This tutorial interacts with the ``TopologyManagerWriteService``, a gRPC service available on the **Admin API** of the participant node.
It assumes that the Admin API is not authenticated via client certificates.

.. toggle::

      .. literalinclude:: CANTON/community/base/src/main/protobuf/com/digitalasset/canton/topology/admin/v30/topology_manager_write_service.proto
         :caption: TopologyManagerWriteService

Python
------

It is recommended to use a dedicated python environment to avoid conflicting dependencies.
Considering using `venv <https://docs.python.org/3/library/venv.html>`_.

.. code-block:: bash

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

Then run the setup script to generate the necessary python files to interact with Canton's gRPC interface:

.. code-block::

    ./setup.sh

Finally, the following imports will be needed:

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
   :start-after: [Imports start]
   :end-before: [Imports end]
   :caption: Python imports

Shell
-----

For a terminal-based approach, install the following tools:

- `openssl <https://www.openssl.org/>`_
- `buf <https://buf.build/docs/cli/installation/>`_
- `jq <https://jqlang.org/>`_
- `xxd <https://linux.die.net/man/1/xxd>`_

The tutorial uses a buf proto image to (de)serialize proto messages.

    .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
       :start-after: [start-docs-entry: set buf image path]
       :end-before: [end-docs-entry: set buf image path]
       :caption: Buf image path

    .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
       :start-after: [start-docs-entry: set artifact root path]
       :end-before: [end-docs-entry: set artifact root path]
       :caption: Artifact root path

The following functions will be used throughout the tutorial:

.. toggle::

    .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
       :start-after: [start byte utility functions]
       :end-before: [end byte utility functions]
       :caption: Utility functions

Error Handling
--------------

When encountering RPC errors, it may be necessary to perform additional deserialization to get actionable information on the cause of the error.
Example of an RPC error:

.. code-block:: json

    {
       "code": "invalid_argument",
       "message": "PROTO_DESERIALIZATION_FAILURE(8,0): Deserialization of protobuf message failed",
       "details": [
          {
             "type": "google.rpc.ErrorInfo",
             "value": "Ch1QUk9UT19ERVNFUklBTElaQVRJT05fRkFJTFVSRRobCgtwYXJ0aWNpcGFudBIMcGFydGljaXBhbnQxGlQKBnJlYXNvbhJKVmFsdWVDb252ZXJzaW9uRXJyb3Ioc3RvcmUsRW1wdHkgc3RyaW5nIGlzIG5vdCBhIHZhbGlkIHVuaXF1ZSBpZGVudGlmaWVyLikaDQoIY2F0ZWdvcnkSATg"
          }
       ]
    }

The ``type`` field specifies the protobuf type in which the error is encoded. In this case, it is a ``google.rpc.ErrorInfo`` message.
The following utility code can be used to deal with errors and extract useful information out of them.

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/utils.sh
           :start-after: [start make_rpc_call fn]
           :end-before: [end make_rpc_call fn]
           :caption: Wrapper function for RPC calls

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/utils.sh
           :start-after: [start handle_rpc_error fn]
           :end-before: [end handle_rpc_error fn]
           :caption: Function to handle common RPC errors

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: handle_grpc_error
           :caption: Function to handle common RPC errors

1. Signing Keys
===============

First, generate an external signing key pair to use in the rest of this tutorial.

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
           :start-after: [start generate keys]
           :end-before: [end generate keys]
           :caption: Generate a signing key pair
           :dedent: 4

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.py
           :start-after: [start generate keys]
           :end-before: [end generate keys]
           :caption: Generate a signing key pair
           :dedent: 4

.. _topology_transaction_hash:

2. Hash
=======

Hashing is required at several steps to compute a hash over a sequence of bytes.
The process uses an underlying algorithm, with specific prefixes added to both the input bytes and the final hash:

1. A hash purpose (a 4-byte integer) is prefixed to the byte sequence. Hash purpose values are defined directly in the Canton codebase.

    .. toggle::

        .. literalinclude:: CANTON/community/base/src/main/scala/com/digitalasset/canton/crypto/HashPurpose.scala
           :caption: HashPurpose
           :language: scala

2. The resulting data is hashed using the underlying algorithm.
3. The final multihash is prefixed again with two bytes, following the `multi-codec <https://github.com/multiformats/multicodec>`_ specification:

    * The identifier for the hash algorithm used.
    * The length of the hash.

.. tip::

    For most practical usages, SHA-256 can be used as the underlying algorithm, and is used in this tutorial as well.

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start compute_canton_hash fn]
           :end-before: [end compute_canton_hash fn]
           :caption: Function to compute a canton compatible sha-256 hash

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: compute_sha256_canton_hash
           :caption: Function to compute a canton compatible sha-256 hash

.. _topology_transaction_fingerprint:

3. Fingerprint
==============

Canton uses fingerprints to efficiently identify and reference signing keys. A fingerprint is a hash of the public key.
Using the hashing algorithm described previously, compute the fingerprint of the public key. For fingerprints, the hash purpose value is ``12``.

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start compute_canton_fingerprint fn]
           :end-before: [end compute_canton_fingerprint fn]
           :caption: Compute fingerprint function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
           :start-after: [start compute fingerprint]
           :end-before: [end compute fingerprint]
           :caption: Compute fingerprint

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: compute_fingerprint
           :caption: Function to compute a canton fingerprint

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.py
           :start-after: [start compute fingerprint]
           :end-before: [end compute fingerprint]
           :caption: Compute fingerprint
           :dedent: 4

.. tip::

    The scripts in this tutorial can provide a quick way to verify third party implementations of the hashing and signing logic.
    For instance, the following script outputs the valid fingerprint of a signing public key passed in a base64 format:

    .. code-block::

        > . ./interactive_topology_util.sh && compute_canton_fingerprint_from_base64 "2RwUiIHVUVdulxzD8NKtPmIaaBqMer1A90rDjoklJPY="
        1220205057e331cc8929dd217e2f8e63f503b7081773de60d01fb46839700bc5caaa

.. _topology_transaction_namespace_mapping:

4. Namespace Delegation Mapping
===============================

There is a number of different mappings available, each modeling a part of the topology state.

.. toggle::

    .. literalinclude:: CANTON/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v30/topology.proto
        :start-after: [docs-entry-start: topology mapping]
        :end-before: [docs-entry-end: topology mapping]
        :caption: Topology mappings

This tutorial illustrates the process of importing a root namespace delegation, represented by the ``NamespaceDelegation`` mapping, but the same procedure can be applied
for any topology mapping.

The Namespace Delegation mapping requires three values:

1. ``namespace``: Root key's fingerprint
2. ``target_key``: Public key expected to be used by delegation. Root namespace delegations are self-signed.

    * The format (``DER``) and specification (``EC256``) of the key must match those of the key generated in step 1.
3. ``is_root_delegation``: ``true`` for root namespace delegations

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start build_namespace_mapping fn]
           :end-before: [end build_namespace_mapping fn]
           :caption: Generate root namespace delegation mapping function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
           :start-after: [start create mapping]
           :end-before: [end create mapping]
           :caption: Generate the mapping
           :dedent: 4

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: build_namespace_mapping
           :caption: Generate root namespace delegation mapping function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.py
           :start-after: [start build mapping]
           :end-before: [end build mapping]
           :caption: Build the mapping
           :dedent: 4

5. Topology Transaction
=======================

The topology state is scoped to a synchronizer. Each synchronizer supports a specific version of the Canton protocol, called ``Protocol Version`` (more details on the :externalref:`versioning page <canton_versioning>`).
When integrating with the topology API, it is important to select which synchronizer the topology changes should target. This is especially relevant when interacting with the topology API of a participant node,
which may be connected to multiple synchronizers at any given time.

Once a synchronizer is selected, its ``ProtocolVersion`` can be retrieved via the ``SequencerConnectService#GetSynchronizerParameters`` RPC of the sequencer API.

.. toggle::

    .. literalinclude:: CANTON/community/base/src/main/protobuf/com/digitalasset/canton/sequencer/api/v30/sequencer_connect_service.proto
       :caption: SequencerConnectService

The Canton console on a sequencer node of the target synchronizer also provides a simple way to get this value:

.. snippet:: external_signing_topology_transaction
    .. hidden:: bootstrap.synchronizer(
          synchronizerName = "da",
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1, mediator1),
          synchronizerThreshold = 2,
          staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
        )
    .. success:: sequencer1.synchronizer_parameters.static.get.protocolVersion

Each ``Protocol Version`` has a corresponding ``Protobuf Version`` for protobuf messages involved in the Canton protocol. That includes the ``TopologyTransaction`` message.

..
    Dynamically generated content:
.. generatedinclude:: topology_versioning.rst.inc

The protobuf version becomes relevant in the :ref:`Version Wrapper section <topology_transaction_version_wrapper>`.

.. note::

    The versioning of protobuf messages is relatively stable and is not expected to change often.
    The rest of the tutorial assumes the protobuf version used is ``30``.

Topology transactions consist of three parts:

Topology Mapping
----------------

See the :ref:`Namespace Delegation Mapping section <topology_transaction_namespace_mapping>`

Serial
------

The ``serial`` is a monotonically increasing number, starting from 1.
Each transaction creating, replacing, or deleting a unique topology mapping must specify a serial incrementing the serial of the previous accepted transaction for that mapping by 1.
Uniqueness is defined differently for each mapping. Refer to the protobuf definition of the mapping for details.
This mechanism ensures that concurrent topology transactions updating the same mapping do not accidentally overwrite each other.
To obtain the serial of an existing transaction, use the ``TopologyManagerReadService`` to list relevant mappings and obtain their current serial.

.. toggle::

    .. literalinclude:: CANTON/community/base/src/main/protobuf/com/digitalasset/canton/topology/admin/v30/topology_manager_read_service.proto
        :caption: TopologyManagerReadService

In this tutorial, it is assumed that the ``NamespaceDelegation`` created is new, in particular there is no pre-existing root namespace delegation with the key created in step 1.
The serial is therefore set to 1.

.. tip::

    For an example of how to read and increment the serial, see the :ref:`external party onboarding tutorial <external_signing_onboarding_serial_update>`

Operation
---------

There are two operations possible:

* ``ADD_REPLACE``: Adds a new mapping or replaces an existing one.
* ``REMOVE``: Remove an existing mapping

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start build_topology_transaction fn]
           :end-before: [end build_topology_transaction fn]
           :caption: Build transaction function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
           :start-after: [start build transaction]
           :end-before: [end build transaction]
           :caption: Build transaction
           :dedent: 4

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: build_topology_transaction
           :caption: Build transaction function

.. _topology_transaction_version_wrapper:

6. Version Wrapper
==================

In order to guarantee backwards compatibility while supporting changes to the protobuf messages involved in the protocol,
Canton wraps serialized messages with a wrapper that includes the protobuf version tied to the message.

.. literalinclude:: CANTON/community/base/src/main/protobuf/com/digitalasset/canton/version/v1/untyped_versioned_message.proto
   :start-after: [start UntypedVersionedMessage]
   :end-before: [end UntypedVersionedMessage]
   :caption: UntypedVersionedMessage

* ``data``: serialized protobuf topology transaction
* ``version``: protobuf version of the topology transaction message

Wrap the serialized transaction in an ``UntypedVersionedMessage``, and serialize the result:

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start build_versioned_transaction fn]
           :end-before: [end build_versioned_transaction fn]
           :caption: Build versioned message function

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start json_to_serialized_versioned_message fn]
           :end-before: [end json_to_serialized_versioned_message fn]
           :caption: Serialize JSON into a versioned protobuf message

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start serialize_topology_transaction fn]
           :end-before: [end serialize_topology_transaction fn]
           :caption: Serialize versioned transaction function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
           :start-after: [start build versioned transaction]
           :end-before: [end build versioned transaction]
           :caption: Build and serialize versioned topology transaction

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: build_versioned_transaction
           :caption: Generate versioned message function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: serialize_topology_transaction
           :caption: Serialize topology transaction function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.py
           :start-after: [start build serialized versioned transaction]
           :end-before: [end build serialized versioned transaction]
           :caption: Serialize versioned topology transaction
           :dedent: 4



7. Transaction Hash
===================

The next step is to compute the hash of the transaction. It is computed from the serialized protobuf of the versioned transaction.
Simply reuse the hashing function defined earlier in the tutorial. This time, the hash purpose value is ``11``.

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start compute_topology_transaction_hash fn]
           :end-before: [end compute_topology_transaction_hash fn]
           :caption: Compute topology transaction hash function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
           :start-after: [start compute transaction hash]
           :end-before: [end compute transaction hash]
           :caption: Compute transaction hash
           :dedent: 4

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: compute_topology_transaction_hash
           :caption: Compute topology transaction hash function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.py
           :start-after: [start compute transaction hash]
           :end-before: [end compute transaction hash]
           :caption: Compute transaction hash
           :dedent: 4

.. tip::

    To facilitate steps ``5`` to ``7``, the topology API offers a ``GenerateTransactions`` RPC to generate the serialized versioned transaction and its hash.
    When using the ``GenerateTransactions`` API, it is strongly recommended to deserialize the returned transaction, validate its content and re-compute its hash,
    to prevent any accidental misuse or adversarial behavior of the participant generating the transaction.

8. Signature
============

The hash is now ready to be signed. For root namespace transactions, there is only one key involved, and it therefore needs only one signature.
Other topology mappings may require additional signatures, either because the mappings themselves contain additional public keys (e.g ``OwnerToKeyMapping``),
or because the authorization rules of the mapping require signatures from several entities (e.g ``PartyToParticipant``).
All transactions, however, require a signature either from the root namespace key of the namespace the transaction is targeting or from a delegated key of that namespace
registered via a (non-root) ``NamespaceDelegation``.
The authorization rules vary by mapping and are out of the scope of this tutorial, but can be found on their protobuf definition.

.. tip::

    The topology API allows authenticating several transactions with a single hash. This is illustrated in the :ref:`external signing onboarding tutorial <tutorial_onboard_external_party>`.

Sign the hash with the private key:

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start sign_hash fn]
           :end-before: [end sign_hash fn]
           :caption: Sign hash function
           :dedent: 4

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
           :start-after: [start sign hash]
           :end-before: [end sign hash]
           :caption: Sign the transaction hash
           :dedent: 4

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: sign_hash
           :caption: Sign hash function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.py
           :start-after: [start sign hash]
           :end-before: [end sign hash]
           :caption: Sign the transaction hash
           :dedent: 4

9. Submit the transaction
=========================

Submit the transaction and its signature. This is done via the ``AddTransactions`` RPC of the ``TopologyManagerWriteService``:

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start build_canton_signature fn]
           :end-before: [end build_canton_signature fn]
           :caption: Function to build a Signature object

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start build_signed_transaction fn]
           :end-before: [end build_signed_transaction fn]
           :caption: Function to build a SignedTopologyTransaction object

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start build_add_transactions_request fn]
           :end-before: [end build_add_transactions_request fn]
           :caption: Function to build the AddTransactions request

       .. code-block:: shell
           :caption: Admin API endpoint

           GRPC_HOST:localhost
           GRPC_PORT:4002

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
           :start-after: [start submit transaction]
           :end-before: [end submit transaction]
           :caption: Submit the transaction to the API

       If everything goes well, ``Transaction submitted successfully`` should be displayed.

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.py
           :start-after: [Create Admin API gRPC Channel]
           :end-before: [Created Admin API gRPC Channel]
           :caption: Create admin gRPC channel
           :dedent: 4

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: build_canton_signature
           :caption: Build canton signature function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: build_signed_transaction
           :caption: Build signed transaction function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: submit_signed_transactions
           :caption: Submit signed transactions function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.py
           :start-after: [start submit request]
           :end-before: [end submit request]
           :caption: Submit the transaction to the API
           :dedent: 4

Proposal
--------

The ``SignedTopologyTransaction`` message contains a boolean ``proposal`` field.
When set to true, it allows submitting topology transactions without attaching all the signatures required for the transaction to be fully authorized.
This is especially useful in cases where signatures from multiple entities of the network are necessary, that would be tedious and difficult to gather offline.

10. Observe the transaction
===========================

The last step of the tutorial is to observe the ``NamespaceDelegation`` on the topology state of the synchronizer.
Note that the submission is asynchronous, which means it may take some time before the submission is accepted.

.. tabs::

   .. group-tab:: Bash

       .. literalinclude:: CANTON/community/app/src/pack/scripts/topology/topology_util.sh
           :start-after: [start list_namespace_delegations fn]
           :end-before: [end list_namespace_delegations fn]
           :caption: Function to build a list namespace delegation request

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.sh
           :start-after: [start observe transaction]
           :end-before: [end observe transaction]
           :caption: Wait to observe the namespace delegation in the topology state of the synchronizer

   .. group-tab:: Python

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_util.py
           :pyobject: list_namespace_delegation
           :caption: List namespace delegation function

       .. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/interactive_topology_example.py
           :start-after: [start observe transaction]
           :end-before: [end observe transaction]
           :caption: Observe the Namespace delegation
           :dedent: 4

This concludes the tutorial. The transaction is now active on the topology state of the synchronizer.
The code used in this tutorial is available in the ``examples/08-interactive-submission`` folder and can be run with

.. tabs::

   .. group-tab:: Bash

       .. code-block:: bash

            ./interactive_topology_example.sh localhost:4002

   .. group-tab:: Python

       .. code-block:: bash

            python interactive_topology_example.py --synchronizer-id da::12207a94aca813c822c6ae10a1b5478c2ba1077447b468cc66dbd255f60f8fa333e1 run-demo
