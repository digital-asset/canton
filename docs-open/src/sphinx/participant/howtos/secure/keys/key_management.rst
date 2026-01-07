..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _key-management:

Crypto key management
=====================

This page explains how to list, rotate, generate, deactivate, and delete keys of a Canton node.
It also explains how to configure the cryptographic schemes supported by a node.

There is a separate section on the :ref:`management of namespace keys<namespace-key-management>`.

List keys
~~~~~~~~~

Use the following command to enumerate the public keys that the node ``myNode`` stores in its vault:

.. snippet:: key_management
    .. hidden:: val myNode = mediator1
    .. success(output=10):: myNode.keys.public.list()

This will show a non-empty sequence of keys, assuming `myNode` is running and has created keys during startup,
which is the default behavior.

Similarly, a user can enumerate the private keys of ``myNode``:

.. snippet:: key_management
    .. success(output=10):: myNode.keys.secret.list()

A user can list the keys that a node has authorized for usage at a Synchronizer like this:

.. snippet:: key_management
    .. hidden:: val mySynchronizerId = sequencer1.synchronizer_id
    .. success(output=10):: myNode.topology.owner_to_key_mappings
                                    .list(store = mySynchronizerId, filterKeyOwnerUid = myNode.id.filterString)
                                    .flatMap(_.item.keys)
    .. assert:: RES.nonEmpty


.. _rotating-canton-keys:

Rotate keys
~~~~~~~~~~~

Canton supports rotating the keys of a node ``myNode`` with a single command:

.. snippet:: key_management
    .. success:: myNode.keys.secret.rotate_node_keys()

In order to ensure continuous operation, the command first activates the new key and only then deactivates the old key.
The command rotates **all** keys of ``myNode``.
But the command does not rotate namespace keys.
The node deactivates the old keys on all Synchronizers, but they remain in the vault of ``myNode``.

The following command rotates a single key of ``myNode``:

.. snippet:: key_management
    .. hidden:: val oldKeyFingerprint = myNode.topology.owner_to_key_mappings
                                           .list(store = mySynchronizerId, filterKeyOwnerUid = myNode.id.filterString)
                                           .head.item
                                           .keys.head.id.unwrap
    .. success:: myNode.keys.secret.rotate_node_key(oldKeyFingerprint, "newKeyName")

.. note::
   Key rotation should be :ref:`interleaved with taking a backup <backup-restore-private-key-rotation>`.

.. todo:: <https://github.com/DACH-NY/canton/issues/27699>
   Update the commands and docs so that one can actually pause in between and take a backup.

.. _deleting-canton-keys:

Generate and activate keys
~~~~~~~~~~~~~~~~~~~~~~~~~~

Canton creates keys with default parameters for all nodes.
To have different key parameters, generate keys manually.

To generate and activate a new signing key for ``myNode``, run the following commands:

.. snippet:: key_management
    .. success:: val key = myNode.keys.secret.generate_signing_key(
          "mySigningKeyName",
          SigningKeyUsage.ProtocolOnly,
          Some(SigningKeySpec.EcCurve25519)
        )
    .. success:: myNode.topology.owner_to_key_mappings.add_key(key.fingerprint, key.purpose)

Likewise, the following commands generate and activate a new encryption key:

.. snippet:: key_management
    .. success:: val key = myNode.keys.secret.generate_encryption_key(
          "myEncryptionKeyName",
          Some(EncryptionKeySpec.EcP256)
        )
    .. success:: myNode.topology.owner_to_key_mappings.add_key(key.fingerprint, key.purpose)

Refer to the page on :ref:`key restrictions<key-restrictions>` for further information on ``SigningKeyUsage``.
Keys are generated according to specific cryptographic key formats. Please refer to the
following :ref:`tables <canton_supported_key_formats>` for more information on the expected key formats in Canton.

Deactivate keys
~~~~~~~~~~~~~~~

To deactivate a key on all Synchronizers run the following command:

.. snippet:: key_management
    .. success:: myNode.topology.owner_to_key_mappings.remove_key(key.fingerprint, key.purpose)

Node operators normally do not need to deactivate keys, as a node deactivates old keys when rolling keys.
When changing to a different scheme, it may be necessary that an operator explicitly deactivates a key.

Delete keys
~~~~~~~~~~~

After rotating or deactivating a key it remains in the node's vault.
To permanently delete a key from the vault, use the following console command:

.. snippet:: key_management
    .. success:: myNode.keys.secret.delete(key.fingerprint, force = true)

.. warning::
  Exercise caution when deleting a private key.
  Ensure that the key is no longer in use and is not needed for decrypting or creating signatures for old messages.
  Therefore, only delete a key after deactivating the key and pruning the node for a timestamp later than the deactivation.
  This is especially crucial for sequencers in open networks.
  For example, if a participant lags in retrieving sequenced events, and the operator of the sequencer rolls the sequencer’s signing key,
  the old signing key must remain accessible to sign the events from before the key
  roll for lagging participants. Otherwise, deleting the key prematurely may cause irreversible issues for these
  participants.

Configure cryptographic schemes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Canton supports several :ref:`cryptographic schemes <crypto-schemes>`.
By default, a node allows for using all cryptographic schemes supported by Canton.

To configure the default and allowed schemes for a node include a snippet like the following into the Canton configuration:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/crypto-schemes.conf

.. note:: Every Synchronizer imposes a minimum set of cryptographic schemes that its members must support.
    If a node does not support this minimum set of schemes, it is unable to connect to the Synchronizer.

Disable Session keys
~~~~~~~~~~~~~~~~~~~~

An explanation of the purpose and security implications of session keys is available
in :ref:`Session Keys <canton-security>`.

While session keys improve performance, they also introduce a security risk, as the keys are stored in memory—even if
only for a short duration. If you prefer to disable session keys and accept the resulting performance degradation,
you can do so by setting the following configurations.

To disable **session encryption keys**:

.. literalinclude:: CANTON/community/app/src/test/resources/session-key-cache.conf
   :language: scala
   :start-after: user-manual-entry-begin: SessionEncryptionKeyConfigDisable
   :end-before: user-manual-entry-end: SessionEncryptionKeyConfigDisable
   :dedent:

To disable **session signing keys**:

.. literalinclude:: CANTON/community/app/src/test/resources/session-key-cache.conf
   :language: scala
   :start-after: user-manual-entry-begin: SessionSigningKeyConfigDisable
   :end-before: user-manual-entry-end: SessionSigningKeyConfigDisable
   :dedent:

Please note that **session signing keys** are only used with an
:ref:`external KMS (Key Management Service) provider <external_key_storage>`.
