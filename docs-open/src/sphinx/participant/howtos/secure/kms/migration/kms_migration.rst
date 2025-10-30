..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

2. :ref:`Migrate to external key storage with a Key Management Service (KMS) <external_key_storage_migration>`

   This approach involves creating a new KMS-enabled Participant and transferring all data (e.g., contracts)
   from the Participant node. **The process must be performed manually by each operator using the provided scripts and functions.**

   It offers a clean and self-contained transition but requires a namespace change and coordination with
   all affected Participant node operators.
