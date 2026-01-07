..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _kms_key_rotation:

Rotate keys with KMS
====================

Canton supports both :ref:`rotating the wrapper key <rotate_wrapper_key>` when using envelope encryption,
and :ref:`rotating Canton keys stored externally in a KMS <rotate_external_keys>`.

For smooth disaster recovery, key rotation must be interleaved with backups as described under :ref:`Backup and Restore <backup-restore-private-key-rotation>`.
