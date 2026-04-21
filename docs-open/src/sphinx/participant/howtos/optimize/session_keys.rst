..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _optimize_session_keys:

Configure session keys
======================

Canton uses session keys to reduce expensive cryptographic operations during protocol execution, improving performance.
There are two types: session encryption keys, which reduce the number of asymmetric encryptions, and
session signing keys, which help avoid frequent calls to external signers such as a KMS.

You can read more about the rationale and security considerations
in :externalref:`Session Keys <overview_session_signing_keys>`.

Currently, **session encryption keys are enabled by default**, whereas **session signing keys**, being directly tied to
a KMS, are **disabled by default**. However, the latter can be enabled when using an external KMS to store private keys
via a configuration parameter.

.. important::

    Extending the lifetime of session keys minimizes the need for repeated key negotiation or remote signing/encryption,
    but it also increases the window during which keys are stored in memory, raising the risk of compromise.

Increase session **encryption** keys lifetime
---------------------------------------------

You can control how long a session encryption key remains active by adjusting the ``expire-after-timeout``
values in your configuration. To globally increase the lifetime of session encryption keys,
increase the ``expire-after-timeout`` for both the ``sender-cache`` and ``receiver-cache``.

.. literalinclude:: CANTON/community/app/src/test/resources/session-key-cache.conf
   :language: scala
   :start-after: user-manual-entry-begin: SessionEncryptionKeyConfig
   :end-before: user-manual-entry-end: SessionEncryptionKeyConfig
   :dedent:

.. _enable_session_signing_keys:

Enable session **signing** keys and increase lifetime
-----------------------------------------------------

When using :ref:`external KMS (Key Management Service) provider <external_key_storage>` you can enable and control how
long session signing keys remain active by setting the ``enabled = true`` and adjusting the ``key-validity-duration``
and the ``key-eviction-period``. The ``key-eviction-period`` should always be longer than the ``key-validity-duration``
and at least as long as the ``defaultMaxSequencingTimeOffset`` and the sum of ``confirmation_response_timeout``
and ``mediator_reaction_timeout``, as configured in
the :externalref:`dynamic Synchronizer parameters <dynamic_synchronizer_parameters>`.
If you want to know more about each configurable parameter and the factors to consider when modifying them,
you can refer to the session signing keys' :externalref:`Configurable Parameters <parametrization_session_signing_keys>`
section.

.. literalinclude:: CANTON/community/app/src/test/resources/session-key-cache.conf
   :language: scala
   :start-after: user-manual-entry-begin: SessionSigningKeyConfig
   :end-before: user-manual-entry-end: SessionSigningKeyConfig
   :dedent:
