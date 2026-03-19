..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _external_key_storage_migration_export_keys:

Migrate to external key storage with a KMS by exporting the key material
========================================================================

A running :ref:`Participant will have a set of asymmetric long-term keys that it uses to perform cryptographic
operations <canton-security>`, and to create its namespace. Migrating a live Participant to use a KMS, instead
of storing its private keys locally, requires a series of steps: (1) move those keys to the KMS, (2) create a new
Participant and configure it to use the KMS with the imported keys, followed by (3) copying the data from the
old Participant to the new. **This migration is only possible if the private key material is accessible**.
You must ensure that the private keys you are going to import have not been compromised.

.. note::
    For a decentralized synchronizer, it is better to onboard a new node with new keys rather than migrating
    existing nodes.

Export Keys
-----------

You must first export all cryptographic keys, for example, to files:

.. snippet:: export_keys
    .. success:: val keyIds = participant1.keys.public.list().map(_.id)
    .. success:: val exportedKeys = keyIds.map(keyId => participant1.keys.secret.download(keyId, password = None))
    .. success:: val parsedKeys = exportedKeys.map { keyPair =>
                     CryptoKeyPair.fromTrustedByteString(keyPair).fold(
                       err => throw new RuntimeException(s"Failed to parse key: $err"),
                       cryptoKey => cryptoKey
                     )
                   }
    .. success:: val privateKeys = parsedKeys.map { parsedKey => parsedKey.privateKey match {
                       case EncryptionPrivateKey(id, _, key, _) => (id, key)
                       case SigningPrivateKey(id, _, key, _, _) => (id, key)
                     }
                   }
    .. success:: import java.nio.file.{Files, Paths}
    .. success:: privateKeys.foreach{ case (keyId, privateKey) => Files.write(Paths.get(s"key_${keyId.unwrap}"), privateKey.toByteArray)}
    .. hidden:: import java.io.File
    .. hidden:: keyIds.foreach(keyId => (new File(s"key_${keyId.unwrap}")).delete())

.. todo:: Improve UX for downloading private keys <https://github.com/DACH-NY/canton/issues/31244>

When exporting keys, make sure to **copy the output from `keyIds` into a file**, as this information is needed
again during import. Note that in this case, setting a password on the exported key **cannot be used**, because the key
is immediately parsed and must be imported into the KMS, and both AWS and GCP KMS do not support
password-based encryption.

Securing Keys with a Wrapping Key
---------------------------------

Before importing these keys into a KMS, you should protect them for transport by encrypting them with a wrapping key
provided by the KMS into which you wish to import the keys. Both AWS and GCP provide a way to securely import
user-supplied cryptographic keys by encrypting them with a wrapping key:

- `AWS KMS <https://docs.aws.amazon.com/kms/latest/developerguide/importing-keys-encrypt-key-material.html>`
- `GCP KMS <https://docs.cloud.google.com/kms/docs/key-wrapping>`

Import Keys to a KMS
--------------------

Importing keys into a KMS must be done manually through the available APIs.
**Please ensure that your keys have the expected format and are supported by the KMS.**

Currently, the two KMSs supported by Canton (apart from any custom KMS implementations) — AWS KMS and GCP KMS — support
all key schemes used by Canton, **except for the ``ECIES`` encryption key scheme**. Since ``ECIES`` encryption keys are
not supported, a new key (e.g., ``RSA``) must be used. This process is explained in more detail
in :ref:`Using new keys <using_new_keys>`.

.. todo::
    Add integration test to check KMS import APIs. <https://github.com/DACH-NY/canton/issues/31233>

Below are instructions for importing keys into:

- `AWS KMS <https://docs.aws.amazon.com/kms/latest/developerguide/importing-keys.html>`
- `GCP KMS <https://docs.cloud.google.com/kms/docs/key-import>`

Take note of the KMS key identifier that was assigned for each imported key so that you can use it later.

Create a new Participant with KMS-enabled
-----------------------------------------

After all keys have been successfully imported into the KMS, you must create and start a new Participant.
**When registering the keys with the KMS, it is important to use all the information from the key export**,
including metadata, usage flags, and any associated identifiers, to ensure the keys are correctly recognized and
functional. The section
:ref:`Enable external key storage with a KMS <external_key_storage>` explains how to configure a Participant to use a
KMS for storing and operating on private keys, while the section
:ref:`Run with manually-generated keys <external_key_management>` explains how to start a Participant with pre-existing
KMS keys.

Replicate data
--------------

Finally, you must transfer all parties and active contracts from the old Participant to the new one.
Since the identities of the two Participants are the same, you do not need to modify any of the data.

First, recreate all parties from the old Participant on the new Participant:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/security/kms/KmsMigrationIntegrationTest.scala
  :language: scala
  :start-after: user-manual-entry-begin: KmsRecreatePartiesInNewParticipant
  :end-before: user-manual-entry-end: KmsRecreatePartiesInNewParticipant
  :dedent:

Next, transfer the active contracts of all parties from the old Participant to the new one.
You can then reconnect to the Synchronizer:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/security/kms/KmsMigrationIntegrationTest.scala
  :language: scala
  :start-after: user-manual-entry-begin: KmsMigrateACSofParties
  :end-before: user-manual-entry-end: KmsMigrateACSofParties
  :dedent:

The result is a new Participant with the same namespace, whose private keys are stored and managed by a KMS,
connected to the same Synchronizer as before.

.. warning::
    AWS and GCP KMS do not allow the retrieval of private keys. Therefore, if you want to be able to revert to not
    using a KMS or to change KMS providers, you must persist your keys in a secure location elsewhere
    (e.g., an encrypted offline backup stored under strict access controls), **in particular your root namespace key**.
    There is currently no automatic revert process. You must follow a similar procedure: obtain the keys,
    :externalref:`manually initialize <canton_supported_schemes>` a new node, transfer data.

.. _using_new_keys:

Using new keys
--------------

Creating new keys directly in the KMS is not possible because the keys must first be announced on the network. Therefore,
the only way to start using a new KMS-backed participant with fresh keys is either to create them before migrating
the node to the KMS or to rotate the keys after the migration is complete using:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/security/kms/RotateKmsKeyIntegrationTest.scala
  :language: scala
  :start-after: user-manual-entry-begin: RotateKmsNodeKey
  :end-before: user-manual-entry-end: RotateKmsNodeKey
  :dedent:

You **cannot use or rotate the namespace root key**, because this key is responsible for identifying a Participant and
creating its namespace.

If you want to use new keys, you must manually :ref:`generate <generate-activate-new-keys>` or
:ref:`rotate <rotating-canton-keys>` them using a scheme that Canton supports. The
tables :externalref:`here <canton_supported_schemes>` provide information about the currently supported crypto schemes
in Canton. Currently, the only crypto scheme that a KMS-backed Participant does not support is encryption
with `ECIES-HMAC-SHA256-AES128-CBC`, and instead it uses `RSA-OAEP-SHA256`.

For example, since `ECIES` keys cannot be imported into one of our supported KMS providers, the operator of the original
non-KMS Participant must create a new `RSA-2048` encryption key and announce it as the Participant’s new encryption key.
Only after this step can you migrate to a KMS-enabled Participant.

The migration process then proceeds in the same way, except that the new target Participant running with a KMS must
ensure it is configured with the correct schemes, in line with the new key that was created.
For example, if you want encryption keys to use `RSA-2048`, you must add the following configuration:

.. literalinclude:: CANTON/community/app/src/test/resources/aws-kms-provider-tagged.conf
   :language: none
   :start-after: user-manual-entry-begin: DefaultCryptoSchemeConfig
   :end-before: user-manual-entry-end: DefaultCryptoSchemeConfig
