..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _key-restrictions:

Restrict key usage
==================

This page explains how to limit the usage of cryptographic keys of a Canton node to specific purposes, as best practice
to contain damage of potentially compromised keys.

.. _signing-key-usage-restrictions:

Signing key usage
-----------------

Canton defines the following key usages for signing keys:

.. todo:: Link to appropriate reference page <https://github.com/DACH-NY/canton/issues/25832>

* ``Namespace``: a key that defines a node's identity and signs topology requests (see :ref:`namespace key management<namespace-key-management>`)
* ``SequencerAuthentication``: a key that authenticates members of the network towards a sequencer
* ``Protocol``: a key that signs various structures as part of the synchronization protocol execution

Restrict the usage of a signing key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. todo:: fix `deleting-canton-keys` and other references in `key_management.rst` <https://github.com/DACH-NY/canton/issues/26155>

Set the ``usage`` parameter with the desired usages when you :ref:`generate a new key<deleting-canton-keys>`.

For example, use the following command to generate a signing key restricted to ``Protocol`` usage only:

.. snippet:: key_restrictions
    .. hidden:: val myNode = mediator1
    .. success:: val myKey = myNode.keys.secret.generate_signing_key(
          name = "mySigningKey",
          usage = SigningKeyUsage.ProtocolOnly,
        )

Some predefined sets are available for common usages: ``NamespaceOnly``, ``SequencerAuthenticationOnly``, ``ProtocolOnly``, ``All``.
Individual usages can otherwise be combined in a ``Set``, but this is not recommended to avoid that keys are used for multiple purposes.

.. note:: Once specified during key generation, the set of usages is fixed and cannot be modified during the lifetime of that key.
    Instead, a new key must be generated and activated, and the old one deactivated. See :ref:`key management<key-management>`
    for more information regarding the key management commands.

If you are using a Key Management Service (KMS) to handle Canton's keys in :ref:`external key storage mode<external_key_storage>`,
and want to use manually generated keys that are already stored in the KMS, set the ``usage`` parameter with the desired usages when you
register the keys with Canton:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/security/KeyManagementIntegrationTest.scala
   :language: scala
   :start-after: user-manual-entry-begin: register_kms_signing_key
   :end-before: user-manual-entry-end: register_kms_signing_key
   :dedent:

The returned key can be used in other key management commands.


Determine the usage of an existing signing key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To find what usage a signing key has been assigned, use the following command:

.. snippet:: key_restrictions
    .. success:: val intermediateKey = myNode.keys.public.list(
          filterFingerprint = myKey.id.unwrap,
        )

If you omit the ``filterFingerprint`` parameter, all the existing keys will be returned.


Determine the existing keys with a given usage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To find all the signing keys that have been assigned a given usage, use the following command:

.. snippet:: key_restrictions
    .. success:: val intermediateKey = myNode.keys.public.list(
          filterUsage = SigningKeyUsage.ProtocolOnly,
        )

All the keys that have been assigned at least one of the usages specified in ``filterUsage`` will be returned.


.. _namespace-key-delegation-restrictions:

Namespace key delegation restrictions
-------------------------------------

As described in :ref:`the identity management page<identity-transactions>`, Canton uses topology transactions to manage identities,
and these transactions are signed by keys with ``Namespace`` usage. :ref:`Namespace key management<namespace-key-management>` shows
that you can create intermediate keys to sign topology transactions, thereby limiting exposure of the root namespace key.
These intermediate keys can be further restricted in terms of which kind of topology transactions they can sign using fine-grained delegation restrictions.

Canton defines the following restrictions for namespace keys:

.. todo:: Link to appropriate reference page and remove start/end markers in `topology.proto` <https://github.com/DACH-NY/canton/issues/25832>

.. literalinclude:: CANTON/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v30/topology.proto
   :start-after: [start-docs-entry: namespace delegation restrictions]
   :end-before: [end-docs-entry: namespace delegation restrictions]

Restrict the types of mappings that a key can sign
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You must first create a new key with at least ``Namespace`` usage, then delegate the signing privileges for a namespace
to that key, specifying the desired restrictions in the ``delegationRestriction`` parameter.

For example, in the following, the first command creates an intermediate key for the root namespace, then the second one
delegates the signing privileges to it, allowing it to sign all types of topology mappings except namespace delegations themselves:

.. snippet:: key_restrictions
    .. success:: val intermediateKey = myNode.keys.secret.generate_signing_key(
          name = "intermediate-key",
          usage = SigningKeyUsage.NamespaceOnly,
        )
    .. success:: myNode.topology.namespace_delegations.propose_delegation(
          namespace = myNode.namespace,
          targetKey = intermediateKey,
          delegationRestriction = CanSignAllButNamespaceDelegations,
        )

Like all topology mappings, the delegation restrictions on a namespace key can be updated.
To do so, issue a new ``propose_delegation`` command with the updated delegation restrictions.

.. warning::
  If you define intermediate namespace keys with restricted delegations, you must ensure that all authorizations are
  covered and that by combining multiple namespace keys together, they can authorize all topology transactions.
  Otherwise, some operational procedures may fail. For example, rotating a key will fail if there is not at least one
  namespace key that can authorize ``OwnerToKey`` mappings.

  In such cases, the operation will fail with a "Could not find an appropriate signing key to issue the topology transaction"
  error message and code ``TOPOLOGY_NO_APPROPRIATE_SIGNING_KEY_IN_STORE``.

Determine the restrictions of an existing namespace key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To find what signing restrictions have been defined on a namespace key, use the following command:

.. snippet:: key_restrictions
    .. success:: myNode.topology.namespace_delegations.list(
          store = TopologyStoreId.Authorized,
          filterTargetKey = Some(intermediateKey.id),
        )

If you omit the ``filterTargetKey`` parameter, all the existing namespace delegations will be returned.
