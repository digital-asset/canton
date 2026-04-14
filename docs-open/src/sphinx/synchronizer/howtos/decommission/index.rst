..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _synchronizer-decommissioning-nodes:

Decommissioning Canton nodes and Synchronizer entities
======================================================

This guide assumes general familiarity with Canton, in particular :externalref:`Canton identity management concepts <identity_management_user_manual>`
and :externalref:`operations from the Canton console <canton_console>`.

Note that, while onboarding new nodes is always possible,
**a decommissioned node or entity is effectively disposed of and cannot rejoin a Synchronizer**.
**Decommissioning is thus an irreversible operation**.

In addition, **decommissioning procedures are currently experimental**; regardless, **backing up nodes to be decommissioned
before decommissioning them is strongly recommended**.

.. _decommissioning-sequencers:

Decommissioning a Sequencer
---------------------------

Sequencers are part of a Synchronizer’s messaging infrastructure and do not store app contracts,
so they are disposable as long as precautions are taken to avoid disrupting the synchronization services.
This means, concretely, ensuring that:

#. No active Participant Node nor active Mediator is connected to the Sequencer to be decommissioned.
#. All active Participant Nodes and Mediators are connected to an active Sequencer.

After that, the Sequencer can be decommissioned by removing it from the Synchronizer’s topology and finally disposed of.

Disconnecting all nodes from the Sequencer to be decommissioned
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Change the Sequencer connection on the Mediators connected to the Sequencer to be decommissioned
  to use another active Sequencer, as per :ref:`mediator connectivity <mediator-connectivity>`:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/offboarding/SequencerOffboardingIntegrationTest.scala
   :language: scala
   :start-after: user-manual-entry-begin: SequencerOffboardingSwitchAwayMediator
   :end-before: user-manual-entry-end: SequencerOffboardingSwitchAwayMediator
   :dedent:

* Reconnect Participant Nodes to the Synchronizer, as described in :externalref:`Synchronizer connectivity <participant_Synchronizer_connectivity>`, using a Sequencer connection
  to another active Sequencer:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/offboarding/SequencerOffboardingIntegrationTest.scala
   :language: scala
   :start-after: user-manual-entry-begin: SequencerOffboardingSwitchAwayParticipant
   :end-before: user-manual-entry-end: SequencerOffboardingSwitchAwayParticipant
   :dedent:

Decommissioning the Sequencer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sequencers are part of the Synchronizer by virtue of having their node ID equal to the Synchronizer id,
which also means they all have the same node ID. Since a Sequencer’s identity is the same as the Synchronizer’s identity,
you should leave identity and namespace mappings intact.

A Sequencer may use its own cryptographic material. In that case, owner-to-key mappings must be removed for the keys it exclusively owns:

#. Find the authentication and protocol keys on the Sequencer to be decommissioned using the :ref:`keys.public.list <keys.public.list>` command.
#. Among those keys, find the ones not shared by other Sequencers. You can do this by issuing the :ref:`keys.public.list <keys.public.list>` command
   on each of them: the keys that appear only on the Sequencer node to be decommissioned
   correspond to its exclusively owned keys.

#. Remove the mappings for its exclusively owned keys using the :ref:`topology.owner_to_key_mappings.remove_key <topology.owner_to_key_mappings.remove_key>`
   command.

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/util/OffboardsSequencerNode.scala
   :language: scala
   :start-after: user-manual-entry-begin: SequencerOffboardingRemoveExclusiveKeys
   :end-before: user-manual-entry-end: SequencerOffboardingRemoveExclusiveKeys
   :dedent:

The cryptographic material exclusively owned by a decommissioned Sequencer must also be disposed of:

- If it was stored only on the decommissioned Sequencer, it must be disposed of together with the decommissioned
  Sequencer node.
- However, if a decommissioned Sequencer’s cryptographic material is managed via a KMS system, it must be disposed of
  through the KMS; refer to your KMS’ documentation and internal procedures to handle this.
  KMS-managed cryptographic material of Sequencer nodes.

A quorum of Synchronizer owners can then propose the Sequencer out of the Synchronizer topology:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/util/OffboardsSequencerNode.scala
   :language: scala
   :start-after: user-manual-entry-begin: SequencerOffboardingRemoveFromTopology
   :end-before: user-manual-entry-end: SequencerOffboardingRemoveFromTopology
   :dedent:

BFT Sequencers must also disappear from the ordering topology, before they are switched off; you can check that
with the :ref:`bft.get_ordering_topology <bft.get_ordering_topology>` command.

.. _decommissioning-mediators:

Decommissioning a Mediator
--------------------------

Mediators are also part of a Synchronizer’s messaging infrastructure and do not store app contracts,
so they are disposable as long as precautions are taken to avoid disrupting the synchronization services.
This means ensuring that at least one Mediator remains on the Synchronizer.

If other mediators exist on the Synchronizer, a mediator can be decommissioned by a quorum of Synchronizer owners
proposing it out of the Synchronizer topology.

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/util/OffboardsMediatorNode.scala
   :language: scala
   :start-after: user-manual-entry-begin: OffboardMediator
   :end-before: user-manual-entry-end: OffboardMediator
   :dedent:
