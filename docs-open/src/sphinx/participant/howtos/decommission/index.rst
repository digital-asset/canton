..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _decommissioning-nodes:

Decommissioning Canton Nodes and Synchronizer entities
======================================================

This guide assumes general familiarity with Canton, in particular :ref:`Canton identity management concepts <identity_management_user_manual>`
and :ref:`operations from the Canton console <canton_console>`.

Note that, while onboarding new nodes is always possible,
**a decommissioned node or entity is effectively disposed of and cannot rejoin a Synchronizer**.
**Decommissioning is thus an irreversible operation**.

In addition, **decommissioning procedures are currently experimental**. Regardless, **backing up nodes to be decommissioned
before decommissioning them is strongly recommended**.

.. _decommissioning-participants:

Decommissioning a Participant Node
----------------------------------

Prerequisites
^^^^^^^^^^^^^

Be mindful that making a Participant Node unavailable (by disconnecting it from the Synchronizer or
decommissioning it) might block other workflows and/or prevent other parties from exercising Choices on
their Contracts.

As an example, consider the following scenario:

* Party `bank` is hosted on Participant Node `P1` and party `alice` is hosted on Participant Node `P2`.
* An active Contract exists with `bank` as signatory and `alice` as observer.
* `P1` is decommissioned.

If `bank` is not multi-hosted, any attempt by `alice` to use the Contract fails because
`bank` cannot confirm. The Contract remains active on `P2` forever unless purged
via the repair service and only non-consuming Choices and fetches can be committed.

Similar considerations apply if `P2` were to be decommissioned, even though `alice` is “only” an observer: if `alice`
is not multi-hosted, the Contract would remain active on `P1` until purged via the repair service and only
non-consuming Choices and fetches could be committed.

Additionally, when `P1` is decommissioned `P2` stops receiving ACS commitments from `P1`, which may prevent pruning.
The same applies in reverse if `P2` is decommissioned.

Thus, properly decommissioning a Participant Node requires the following high-level steps:

1. *Ensuring that the prerequisites are met*: ensure that active Contracts and workflows using them are not "stuck" due to parties required to operate on them becoming unavailable.

.. note::
   More specifically, for a Contract Action to be committed:

   - For “Create” Actions all stakeholders must be hosted on active Participant Nodes.
   - For consuming “Exercise” Actions all stakeholders, actors, Choice observers,
     and Choice authorizers must be hosted on active Participant Nodes.

   The exact prerequisites to be met to decommission a Participant Node therefore depend on the design
   of the Daml app and should be accounted and tested for in the initial Daml design process.

2. *Decommissioning*: remove the Participant Node from the topology state.

After that, the Participant Node can be disposed of.

Decommissioning a Participant Node once the prerequisites are met
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Stop apps from sending commands to the Ledger API of the Participant Node to be decommissioned
   to avoid failed commands and errors.
#. Disconnect the Participant Node to be decommissioned from all Synchronizers as described in
   :ref:`enabling and disabling connections <connectivity_participant_reconnect>`.
#. Use the :ref:`topology.participant_synchronizer_permissions.propose <topology.participant_synchronizer_permissions.propose>` command
   to fully unauthorize the Participant Node on the Synchronizer:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/offboarding/ParticipantOffboardingIntegrationTest.scala
  :language: scala
  :start-after: user-manual-entry-begin: OffboardParticipant
  :end-before: user-manual-entry-end: OffboardParticipant
  :dedent:

Finally use the :ref:`repair.disable_member <repair.disable_member>` command to disable the Participant Node
being decommissioned in all Sequencers and remove any Sequencer data associated with it.

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/offboarding/ParticipantOffboardingIntegrationTest.scala
  :language: scala
  :start-after: user-manual-entry-begin: DisableParticipant
  :end-before: user-manual-entry-end: DisableParticipant
  :dedent:
