..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _kms_mode:

Select a KMS mode of operation
==============================

Canton supports using a Key Management Service (KMS) to increase the security of
stored private keys.
For Canton to actually use a KMS, you need to decide and configure one of the
**two independent ways** to use this service:

1. :ref:`Enable encrypted private key storage <encrypted_key_storage>`

   In this mode, Canton generates the private keys internally, and the KMS is used only to protect those keys at rest (i.e., the keys are encrypted before being stored in Canton’s database).
   This offers an additional layer of security without requiring external key generation.

2. :ref:`Enable external key storage <external_key_storage>`

   In this mode, the private keys are generated and stored entirely within the KMS.
   Canton never sees the raw private key material and interacts with the KMS only to perform cryptographic operations.

.. note::

   Throughout this documentation, these modes might be referred to as **envelope encryption** and **external keys**, respectively.


