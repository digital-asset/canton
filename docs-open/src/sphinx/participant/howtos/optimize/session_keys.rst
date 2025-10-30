..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _optimize_session_keys:

Configure session keys
======================

Canton uses session keys to reduce expensive cryptographic operations during protocol execution, improving performance.
There are two types: session encryption keys, which reduce the number of asymmetric encryptions, and
session signing keys, which help avoid frequent calls to external signers such as a KMS.

You can read more about the rationale and security considerations in :ref:`Session Keys <canton-security>`.

Extending the lifetime of session keys minimizes the need for repeated key negotiation or remote signing—but it also
increases the window during which keys are stored in memory, raising the risk of compromise.

Increase session **encryption** keys lifetime
---------------------------------------------

You can control how long a session encryption key remains active by adjusting the ``expire-after-timeout``
values in your configuration. To globally increase the lifetime of session encryption keys,
increase the ``expire-after-timeout`` for both the ``sender-cache`` and ``receiver-cache``.

.. literalinclude:: CANTON/enterprise/app/src/test/resources/session-key-cache.conf
   :language: scala
   :start-after: user-manual-entry-begin: SessionEncryptionKeyConfig
   :end-before: user-manual-entry-end: SessionEncryptionKeyConfig
   :dedent:

Increase session **signing** keys lifetime
------------------------------------------

When using :ref:`external KMS (Key Management Service) provider <external_key_storage>` you can control how
long a session signing key remains active by adjusting the ``key-validity-duration``
and the ``key-eviction-period``. The ``key-eviction-period`` should always be longer than the ``key-validity-duration``
and at least as long as the sum of ``confirmation_response_timeout`` and ``mediator_reaction_timeout``, as configured
in the :externalref:`dynamic Synchronizer parameters <dynamic_synchronizer_parameters>`.

.. literalinclude:: CANTON/enterprise/app/src/test/resources/session-key-cache.conf
   :language: scala
   :start-after: user-manual-entry-begin: SessionSigningKeyConfig
   :end-before: user-manual-entry-end: SessionSigningKeyConfig
   :dedent:
