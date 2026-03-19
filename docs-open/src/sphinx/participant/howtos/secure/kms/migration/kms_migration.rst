..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _kms_migration:

Migrate to a KMS
================

This section outlines the steps required to migrate from a running non-KMS participant to one that is KMS-enabled,
as well as the interoperability between nodes that use KMS and those that do not.
The migration procedure depends on the selected mode of operation:

1. :ref:`Migrate to encrypted private key storage with a Key Management Service (KMS) <encrypted_key_storage_migration>`

   This process requires configuring the node to use a symmetric wrapper key. **Migration is done automatically
   after this configuration step.**

2. :ref:`Migrate to external key storage with a Key Management Service (KMS) by exporting the key material <external_key_storage_migration_export_keys>`

   This migration consists of exporting the key material (i.e., private and public keys) from the node and
   directly importing it into the target KMS.

   The parties of the original participant and their ACSs are replicated to a new node and configured to use the KMS
   along with the previously imported key material.

   This approach provides a clean and self-contained transition with no namespace change, but it does require
   participant node operators to have access to the private key material for their keys. This also implies that the
   private key material must be stored outside of a KMS, which introduces additional security risks related to key
   custody, storage, and potential exposure.

2. :ref:`Migrate to external key storage with a Key Management Service (KMS) with asset transfer <external_key_storage_migration_transfer_assets>`

  **External parties** can be simply :externalref:`replicated <party-replication>` on the new Participant, and their
  owned contracts can be moved without any model-specific transfers, since these parties have their own keys and
  namespace and do not rely on the Participant's keys.

  **For local parties**, this approach involves creating a new KMS-enabled Participant and transferring all data
  (e.g., contracts) from the original Participant. Transferring parties and ownership of all contracts to
  the new Participant must be done manually through model-specific transfers. This approach can be highly involved
  and **complex, especially if locally hosted parties exist,** and it must be adapted case by case depending on the
  specific contracts and parties owned by the participant. Migrating this way, when locally hosted parties exist,
  should only be done if exporting the key material is not possible or is deemed insecure.
