..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _external_key_storage_migration:

.. wip::
    `#25809: Update this based on new KMS migration guide that still needs to be tested for
    3.3 <https://github.com/DACH-NY/canton/issues/25809>`_

Migrate to external key storage with a KMS
==========================================

After creating a new Participant with the right configuration that connects to a KMS-compatible
Synchronizer
(e.g. :ref:`running KMS or JCE with KMS-supported encryption and signing keys <external_key_storage_interoperability>`),
you must transfer all parties, active contracts and DARs
from the old Participant to the new one. The identities of the two Participants are different so you need to
rewrite your contracts to refer to the new party ids. With this method, you can easily migrate your old Participant
that runs an older protocol version to a new Participant with KMS enabled, that can run a more recent
protocol version. Furthermore, you do not need to safe-keep the old node's root namespace key, because
your Participant namespace changes. However, for it to work a single operator must control
all the contracts or all Participant operators have to agree on this rewrite.

.. warning::

    This only works for a single operator or if all other Participant operators agree and
    follow the same steps.

First, you must recreate all parties of the old Participant in the new Participant, keeping the same display name,
but resulting in different ids due to the new namespace key:

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/enterprise/app/src/test/scala/com/digitalasset/canton/integration/tests/security/kms/KmsMigrationWithNewNamespaceIntegrationTest.scala
    language: scala
    start-after: user-manual-entry-begin: KmsRecreatePartiesInNewParticipantNewNs
    end-before: user-manual-entry-end: KmsRecreatePartiesInNewParticipantNewNs
    dedent:

Secondly, you should migrate your DARs to the new Participant:

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/enterprise/app/src/test/scala/com/digitalasset/canton/integration/tests/security/kms/KmsMigrationWithNewNamespaceIntegrationTest.scala
    language: scala
    start-after: user-manual-entry-begin: KmsMigrateDarsNewNs
    end-before: user-manual-entry-end: KmsMigrateDarsNewNs
    dedent:

Finally, you need to transfer the active contracts of all the parties from the old Participant to the new one and
rewrite the party ids mentioned in those contracts to match the new party ids. You can then connect to the new Synchronizer:

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/enterprise/app/src/test/scala/com/digitalasset/canton/integration/tests/security/kms/KmsMigrationWithNewNamespaceIntegrationTest.scala
    language: scala
    start-after: user-manual-entry-begin: KmsMigrateACSofPartiesNewNs
    end-before: user-manual-entry-end: KmsMigrateACSofPartiesNewNs
    dedent:

The result is a new Participant node, in a different namespace, with its keys stored and managed by a KMS connected to a Synchronizer
that can communicate using the appropriate cryptographic schemes.

You need to follow the same steps if you want to migrate a Participant back to using a `non-KMS` provider.

.. _external_key_storage_interoperability:

Interoperability with other nodes
---------------------------------

By default, Canton nodes use the ``jce`` crypto provider, which is compatible with other nodes that use external KMS providers
to store Canton private keys. If you change the default ``jce`` provider and use different cryptographic schemes,
you must ensure that it supports the same algorithms and key schemes as the KMS providers, in order to interoperate with other
KMS-enabled Canton nodes.

See :ref:`this table <canton_supported_schemes>` and :ref:`this table <canton_default_schemes>` for a description of
the cryptographic schemes supported by a KMS provider and the ones used by default.
