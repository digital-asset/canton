..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _kms:

Key management service (KMS) configuration
==========================================

By default Canton keys are generated in the node and stored in the node's primary storage.
We currently support a version of Canton that can use a KMS to either:
(a) protect Canton's private keys at rest or (b) protect the private keys both at
rest and at use by storing the keys in the KMS.

Throughout these sections, we will sometimes refer to option (a) as **envelope encryption**,
and option (b) as **external keys**.

You can find more background information on this key management feature in
:ref:`Secure cryptographic private key storage <kms_architecture>`.
See :ref:`Protect private keys with envelope encryption and a Key Management Service <kms_envelope_architecture>`
if you want to know how Canton can protect private keys while they remain internally stored in Canton using a KMS, or
:ref:`Externalize private keys with a Key Management Service <kms_external_architecture>`
for more details on how Canton can enable private keys to be generated and stored by an external KMS.

The following sections focus on how to set up a Participant Node to run with a KMS; however,
most configurations also apply to Sequencer and Mediator.

1. :ref:`Configure a KMS <kms_configuration>`

   We currently support three alternatives:

   a. :ref:`AWS KMS <kms_aws_config>`\*
   b. :ref:`GCP KMS <kms_gcp_config>`\*
   c. :ref:`Driver(-based) KMS <kms_driver_config>` allows users to integrate their own KMS provider by implementing the necessary hooks using Canton’s KMS Driver API. More information on how to implement a Canton KMS Driver can be found in the :ref:`Canton KMS Driver developer guide <kms_driver_guide>`.

.. raw:: html

   <p style="font-size: smaller; font-style: italic;">* only available in the Enterprise Edition.</p>

2. :ref:`Select the mode of operation <kms_mode>`

   You can choose between:

   a. :ref:`Enable encrypted private key storage with KMS <encrypted_key_storage>` – sometimes referred to as *envelope encryption*, protects Canton’s private keys only at rest.
   b. :ref:`Enable external key storage with a KMS <external_key_storage>` – sometimes referred to as *external KMS keys*, private keys are generated and stored entirely within a KMS.

4. :ref:`Migrate to a KMS <kms_migration>`

   How to migrate between a non-KMS and a KMS node, and vice-versa.

5. :ref:`Rotate keys with a KMS <kms_key_rotation>`

   How to rotate existing KMS-managed keys using Canton console commands.


