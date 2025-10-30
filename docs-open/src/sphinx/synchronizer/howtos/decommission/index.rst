..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Review and update the existing content.
    Cover sequencer and mediator removal.
    BFT orderer remove peer endpoint.

.. _synchronizer-decommissioning-nodes:

Decommissioning Canton nodes and Synchronizer entities
======================================================

This guide assumes general familiarity with Canton, in particular :externalref:`Canton identity management concepts <identity_management_user_manual>`
and :externalref:`operations from the Canton console <canton_console>`.

Note that, while onboarding new nodes is always possible,
**a decommissioned node or entity is effectively disposed of and cannot rejoin a synchronizer**.
**Decommissioning is thus an irreversible operation**.

In addition, **decommissioning procedures are currently experimental**; regardless, **backing up nodes to be decommissioned
before decommissioning them is strongly recommended**.

.. _decommissioning-sequencers:

Decommissioning a Sequencer
---------------------------

Sequencers are part of a synchronizer’s messaging infrastructure and do not store application contracts,
so they are disposable as long as precautions are taken to avoid disrupting the synchronization services.
This means, concretely, ensuring that:

#. No active participant nor active mediator is connected to the sequencer to be decommissioned.
#. All active participants and mediators are connected to an active sequencer.

After that, the sequencer can be decommissioned by removing it from the synchronizer’s topology and finally disposed of.

Disconnecting all nodes from the sequencer to be decommissioned
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Change the sequencer connection on the mediators connected to the sequencer to be decommissioned
  to use another active sequencer, as per :ref:`mediator connectivity <mediator-connectivity>`:

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/enterprise/app/src/test/scala/com/digitalasset/canton/integration/tests/offboarding/SequencerOffboardingIntegrationTest.scala
    language: scala
    start-after: user-manual-entry-begin: SequencerOffboardingSwitchAwayMediator
    end-before: user-manual-entry-end: SequencerOffboardingSwitchAwayMediator
    dedent:

* Reconnect participants to the Synchronizer, as described in :externalref:`Synchronizer connectivity <participant_synchronizer_connectivity>`, using a sequencer connection
  to another active sequencer:

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/enterprise/app/src/test/scala/com/digitalasset/canton/integration/tests/offboarding/SequencerOffboardingIntegrationTest.scala
    language: scala
    start-after: user-manual-entry-begin: SequencerOffboardingSwitchAwayParticipant
    end-before: user-manual-entry-end: SequencerOffboardingSwitchAwayParticipant
    dedent:

Decommissioning the sequencer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sequencers are part of the synchronizer by virtue of having their node ID equal to the synchronizer id,
which also means they all have the same node ID. Since a sequencer’s identity is the same as the synchronizer’s identity,
you should leave identity and namespace mappings intact.

However, a sequencer may use its own cryptographic material distinct from other sequencers.
In that case, owner-to-key mappings must be removed for the keys it exclusively owns:

#. Find the keys on the sequencer to be decommissioned using the :externalref:`keys.secret.list <keys.secret.list>` command.
#. Among those keys, find the ones not shared by other sequencers. You can do this by issuing the :externalref:`keys.secret.list <keys.secret.list>` command
   on each of them: the fingerprints that appear only on the sequencer node to be decommissioned
   correspond to its exclusively-owned keys.

.. todo::
    `#22917: Fix broken ref <https://github.com/DACH-NY/canton/issues/22917>`_
    #. Remove the mappings for its exclusively owned keys using the ref:`topology.owner_to_key_mappings.authorize <topology.owner_to_key_mappings.authorize>`
    command.

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/enterprise/app/src/test/scala/com/digitalasset/canton/integration/tests/offboarding/SequencerOffboardingIntegrationTest.scala
    language: scala
    start-after: user-manual-entry-begin: SequencerOffboardingRemoveExclusiveKeys
    end-before: user-manual-entry-end: SequencerOffboardingRemoveExclusiveKeys
    dedent:

Finally, the cryptographic material exclusively owned by a decommissioned sequencer must also be disposed of:

- If it was stored only on the decommissioned sequencer, it must be disposed of together with the decommissioned
  sequencer node.
- However, if a decommissioned sequencer’s cryptographic material is managed via a KMS system, it must be disposed of
  through the KMS; refer to your KMS’ documentation and internal procedures to handle this.
  KMS-managed cryptographic material of sequencer nodes.

.. _decommissioning-mediators:

Decommissioning a Mediator
--------------------------

Mediators are also part of a synchronizer’s messaging infrastructure and do not store application contracts,
so they are disposable as long as precautions are taken to avoid disrupting the synchronization services.
This means ensuring that at least one mediator remains on the synchronizer.

.. todo::
    `#22917: Fix broken ref <https://github.com/DACH-NY/canton/issues/22917>`_
    If other mediators exist on the synchronizer, a mediator can be decommissioned using a single console command
    ref:`setup.offboard_mediator <setup.offboard_mediator>`.

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/enterprise/app/src/test/scala/com/digitalasset/canton/integration/tests/offboarding/MediatorOffboardingIntegrationTest.scala
    language: scala
    start-after: user-manual-entry-begin: OffboardMediator
    end-before: user-manual-entry-end: OffboardMediator
    dedent:
