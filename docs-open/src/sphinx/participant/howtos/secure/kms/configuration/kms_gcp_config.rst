..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _kms_gcp_config:

Configure a Google Cloud Provider (GCP) KMS
===========================================

Like other Canton capabilities, GCP KMS configuration is enabled within a Canton
node's configuration file. A KMS for GCP is configured in the following way:

.. literalinclude:: CANTON/community/app/src/test/resources/encrypted-store-enabled-tagged.conf
   :language: none
   :start-after: user-manual-entry-begin: GcpKmsConfig
   :end-before: user-manual-entry-end: GcpKmsConfig

- ``type`` specifies which KMS to use.
- ``location-id`` specifies which region the GCP KMS is bound to.
- ``project-id`` specifies which project are we binding to.
- ``key-ring-id`` specifies the keyring to use. Multi region keys are enabled for an entire keyring. Therefore, the KMS operator is responsible for setting the keyring correctly depending on the systems' needs.
- ``audit-logging`` flag that enables logging of every call made to the GCP KMS

Configure GCP credentials and permissions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For GCP, Canton uses a `GCP service account
<https://cloud.google.com/docs/authentication/provide-credentials-adc#local-user-cred>`_. For example,
the standard environment variable `GOOGLE_APPLICATION_CREDENTIALS` can be used after
setting up a local Application Default Credentials (ADC) file for the service account. For supported environments,
e.g. GKE, Canton can pick up credentials from the metadata service
`Application Default Credentials <https://cloud.google.com/docs/authentication/application-default-credentials>`_.

The protection and rotation of
the credentials for GCP are the responsibility of the node operator.

The authorized actions, such as IAM permissions, required for GCP KMS depend on the
selected :ref:`mode of operation <kms_mode>` in Canton.

Permissions for envelope encryption
-----------------------------------

The following IAM permissions are required when using **envelope encryption**:

- ``cloudkms.cryptoKeyVersions.create``
- ``cloudkms.cryptoKeyVersions.useToEncrypt``
- ``cloudkms.cryptoKeyVersions.useToDecrypt``
- ``cloudkms.cryptoKeys.get``

.. note::

   If you use cross-project keys, you do **not** need the ``cloudkms.cryptoKeyVersions.create`` permission.

Permissions for external KMS
----------------------------

The following IAM permissions are required when using an **external KMS**, where keys are fully managed and used directly from GCP KMS:

- ``cloudkms.cryptoKeyVersions.create``
- ``cloudkms.cryptoKeyVersions.useToDecrypt``
- ``cloudkms.cryptoKeyVersions.useToSign``
- ``cloudkms.cryptoKeyVersions.get``
- ``cloudkms.cryptoKeyVersions.viewPublicKey``

.. note::

   If you use cross-project keys, you do **not** need the ``cloudkms.cryptoKeyVersions.create`` permission.

Auditability
^^^^^^^^^^^^

GCP provides tools to monitor KMS keys. For GCP logging information you can refer to the `GCP official documentation
<https://cloud.google.com/kms/docs/audit-logging>`_.
Canton logs errors resulting from the use of KMS keys.

Logging
^^^^^^^

For further auditability, Canton can be configured to log every call made to the GCP KMS.
To enable this feature, set the ``audit-logging`` field of the KMS configuration to ``true``.
By default, when using a file-based logging configuration, such logs are written into the main Canton log file.
To write them to a dedicated log file, set the ``KMS_LOG_FILE_NAME`` environment variable or ``--kms-log-file-name`` CLI
flag to the path of the file.
These and other parameters can be configured using environment variables or CLI flags:

.. list-table:: KMS logging configuration
   :widths: 20 20 40 20
   :header-rows: 1

   * - Environment variable
     - CLI Flag
     - Purpose
     - Default
   * - KMS_LOG_FILE_NAME
     - --kms-log-file-name
     - Path to a dedicated KMS log file
     - not set
   * - KMS_LOG_IMMEDIATE_FLUSH
     - --kms-log-immediate-flush
     - When true, logs will be immediately flushed to the KMS log file
     - true
   * - KMS_LOG_FILE_ROLLING_PATTERN
     - --kms-log-file-rolling-pattern
     - Pattern to use when using the rolling file strategy to roll KMS log files
     - yyyy-MM-dd
   * - KMS_LOG_FILE_HISTORY
     - --kms-log-file-history
     - Maximum number of KMS log files to keep when using the rolling file strategy
     - 0 (i.e. no limit)

Sample of a GCP KMS audit log:

.. code-block:: none

    2023-09-12 15:44:54,426 [env-execution-context-27] INFO  c.d.c.c.k.g.a.GcpRequestResponseLogger:participant=participant1 tid:40d47592f1bd50f37e6804fbdff404dd - Sending request [67d92ffb-438b-4dd5-8175-7a54ced7ac3a]: DecryptRequest(CiphertextBlob=** Ciphertext placeholder **, KeyId=canton-kms-test-key, EncryptionAlgorithm=RSAES_OAEP_SHA_256).
    2023-09-12 15:44:54,538 [env-execution-context-32] INFO  c.d.c.c.k.g.a.GcpRequestResponseLogger:participant=participant1 tid:40d47592f1bd50f37e6804fbdff404dd - Received response DecryptResponse(Plaintext=** Redacted plaintext placeholder **, KeyId=canton-kms-test-key, EncryptionAlgorithm=RSAES_OAEP_SHA_256). Original request [67d92ffb-438b-4dd5-8175-7a54ced7ac3a]
    2023-09-12 15:44:54,441 [env-execution-context-138] INFO  c.d.c.c.k.g.a.GcpRequestResponseLogger:participant=participant1 tid:40d47592f1bd50f37e6804fbdff404dd - Sending request [b3aa6202-1734-4751-8ae0-55b7d15c2abb]: SignRequest(KeyId=canton-kms-test-key, Message=** Sign message text placeholder **, MessageType=RAW, SigningAlgorithm=ECDSA_SHA_256).
    2023-09-12 15:44:54,554 [env-execution-context-145] INFO  c.d.c.c.k.g.a.GcpRequestResponseLogger:participant=participant1 tid:40d47592f1bd50f37e6804fbdff404dd - Received response SignResponse(KeyId=canton-kms-test-key, Signature=** Signature message text placeholder **, SigningAlgorithm=ECDSA_SHA_256). Original request [b3aa6202-1734-4751-8ae0-55b7d15c2abb]

Note that sensitive data is removed before logging.
The general log format is as follows:

``tid:<canton_trace_id> - Sending request [<canton_kms_request_id>]: <request details>``
``tid:<canton_trace_id> - Received response <response details>. Original request [<canton_kms_request_id>]``
