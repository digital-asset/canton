..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _external_key_storage:

Enable external key storage with a KMS
======================================

This section shows how to enable external key storage for a Canton Participant,
so that private keys are stored and managed by a KMS, and all cryptographic operations using those keys
must go through the KMS.

.. note::
   This assumes the KMS provider has already been configured.
   See :ref:`Configure a KMS <kms_configuration>`.

To enable external key storage and usage, apply the configuration below before the initial bootstrap of a new
Participant node. **If you're updating an existing Participant, you must instead follow the migration guide:**
:ref:`Migrate to external key storage with a KMS <external_key_storage_migration>`.

.. literalinclude:: CANTON/enterprise/app/src/test/resources/aws-kms-provider-tagged.conf
   :language: none
   :start-after: user-manual-entry-begin: KmsProviderConfig
   :end-before: user-manual-entry-end: KmsProviderConfig

A complete example configuration that puts together both AWS KMS configuration and external
key storage configuration is shown below:

.. literalinclude:: CANTON/enterprise/app/src/test/resources/aws-kms-provider-tagged.conf
   :language: none
   :start-after: user-manual-entry-begin: AwsKmsProviderFullConfig
   :end-before: user-manual-entry-end: AwsKmsProviderFullConfig

The same configuration is applicable for all KMS types, by selecting the correct `type`
(:ref:`AWS <kms_aws_config>`, :ref:`GCP <kms_gcp_config>` or :ref:`Driver <kms_driver_config>`).

This configuration tells Canton that we want to use external keys in a KMS, which are **automatically created by
default when a Participant starts — no further action is required.**
Read on if you prefer to use your own manually generated keys that are already stored in the KMS.

.. _external_key_management:

Run with manually-generated keys
--------------------------------

This howto shows how to configure a participant node with keys that are generated manually in the KMS and
not rely on automatic key generation by Canton

The first step is to manually generate new keys for the Participant in your KMS. All the necessary keys are
listed below and include:

- a (signing) namespace key;
- a protocol signing key;
- a sequencer authentication key;
- an asymmetric encryption key;

Generate each key in your KMS with a supported key algorithm and purpose as shown in Table
:ref:`Key configuration for external keys <key_configuration_external_keys>` and record the generated keys identifier.

To be able to use these keys, you must configure manual initialization in the Participant.
This is explained in detail in :ref:`Initialize node identity manually <setup-manual-identity>`.
However, it is important to note that contrary to what is described there, the keys
are expected to be registered, not generated. Each key must be assigned the correct usage
(see :ref:`Signing Key Usage <signing-key-usage-restrictions>`).

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/topology/TopologyManagementHelper.scala
   :language: scala
   :start-after: user-manual-entry-begin: ManualRegisterKmsKeys
   :end-before: user-manual-entry-end: ManualRegisterKmsKeys
   :dedent:

where `xyzKmsKeyId` is the KMS key identifier for a specific key (e.g. `KMS Key ARN`).

.. note::
    When using `AWS cross account keys <https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-modifying-external-accounts.html>`_
    the key ID can't be used, use the key `ARN` instead.
