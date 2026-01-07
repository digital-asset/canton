..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _encrypted_key_storage_migration:

Migrate to encrypted private key storage with KMS
=================================================

To migrate from a non-encrypted key storage to an encrypted private key storage using a KMS with an
externally hosted symmetric wrapper key, you only need to configure a Participant (or any other node)
to operate in this mode, as explained :ref:`here <encrypted_key_storage>`. The process is
seamless — after restarting the Participant, the new configuration is picked up and Canton's private keys are
automatically encrypted and stored.

Revert encrypted private key storage
------------------------------------

Encrypted private key storage can be reverted back to unencrypted storage.
To prevent accidental reverts, simply deleting the `private-key-store` configuration does **not** revert
to unencrypted storage. Instead, the following configuration must be added, and the node restarted:

.. literalinclude:: CANTON/community/app/src/test/resources/encrypted-store-reverted.conf
   :language: none
   :start-after: user-manual-entry-begin: EncryptedStoreReverted
   :end-before: user-manual-entry-end: EncryptedStoreReverted

.. warning::
    This forces Canton to decrypt its private keys and store them in clear; it is not recommended.

Encrypted private key storage can be enabled again by deleting the ``reverted`` field and reconfiguring the KMS.
