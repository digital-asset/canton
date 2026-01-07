..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _rotate_external_keys:

Rotate external KMS keys
========================

Canton keys can still be manually rotated even if they are externally stored in a KMS.
To do that, you can use the :ref:`standard rotate key commands <rotating-canton-keys>`, or
**if you already have a pre-generated KMS key to rotate to**, run the following command:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/security/kms/RotateKmsKeyIntegrationTest.scala
   :language: scala
   :start-after: user-manual-entry-begin: RotateKmsNodeKey
   :end-before: user-manual-entry-end: RotateKmsNodeKey
   :dedent:

- `fingerprint` - the fingerprint of the key we want to rotate.
- `newKmsKeyId` - the id of the new KMS key (e.g. Resource Name).
- `name` - an optional name for the new key.

No current KMS service offers automatic rotation of asymmetric keys so
the node operator needs to be responsible for periodically rotating these keys.
