..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Cover / discuss decomissioning of the following and their caveats:
    - parties
    - packages
    - crypto keys (link to key rotation in "secure")
    - participant nodes

.. _decommissioning-nodes:

Decommissioning Canton Nodes and Synchronizer Entities
======================================================

This guide assumes general familiarity with Canton, in particular :ref:`Canton identity management concepts <identity_management_user_manual>`
and :ref:`operations from the Canton console <canton_console>`.

Note that, while onboarding new nodes is always possible,
**a decommissioned node or entity is effectively disposed of and cannot rejoin a synchronizer**.
**Decommissioning is thus an irreversible operation**.

In addition, **decommissioning procedures are currently experimental**; regardless, **backing up nodes to be decommissioned
before decommissioning them is strongly recommended**.

.. _decommissioning-participants:

Decommissioning a Participant Node
----------------------------------

Prerequisites
^^^^^^^^^^^^^

Be mindful that making a participant unavailable (by disconnecting it from the synchronizer or
decommissioning it) might block other workflows and/or prevent other parties from exercising choices on
their contracts.

As an example, consider the following scenario:

* Party `bank` is hosted on participant `P1` and party `alice` is hosted on participant `P2`.
* An active contract exists with `bank` as signatory and `alice` as observer.
* `P1` is decommissioned.

If `bank` is not multi-hosted, any attempt by `alice` to use the contract fails because
`bank` cannot confirm. The contract remains active on `P2` forever unless purged
via the repair service and only non-consuming choices and fetches can be committed.

Similar considerations apply if `P2` were to be decommissioned, even though `alice` is “only” an observer: if `alice`
is not multi-hosted, the contract would remain active on `P1` until purged via the repair service and only
non-consuming choices and fetches could be committed.

Additionally, when `P1` is decommissioned `P2` stops receiving ACS commitments from `P1`, which may prevent pruning.
The same applies in reverse if `P2` is decommissioned.

Thus, properly decommissioning a participant requires the following high-level steps:

1. *Ensuring that the prerequisites are met*: ensure that active contracts and workflows using them are not "stuck" due to parties required to operate on them becoming unavailable.

.. note::
   More specifically, for a contract action to be committed:

   - For “create” actions all stakeholders must be hosted on active participants.
   - For consuming “exercise” actions all stakeholders, actors, choice observers,
     and choice authorizers must be hosted on active participants.

   The definition of “informee” is covered by the :externalref:`ledger privacy model <privacy>` section.

   The exact prerequisites to be met in order to decommission a participant therefore depend on the design
   of the Daml application and should be accounted and tested for in the initial Daml design process.

2. *Decommissioning*: remove the participant from the topology state.

After that, the participant can be disposed of.

Decommissioning a participant once the prerequisites are met
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Stop applications from sending commands to the Ledger API of the participant to be decommissioned
   to avoid failed commands and errors.
#. Disconnect the participant to be decommissioned from all synchronizers as described in
   :ref:`enabling and disabling connections <connectivity_participant_reconnect>`.

.. todo::
    `#22917: Fix broken ref <https://github.com/DACH-NY/canton/issues/22917>`_
    #. Use the ref:`sequencer.disable_member <sequencer.disable_member>` command to disable the participant
    being decommissioned in all sequencers and remove any sequencer data associated with it.
    #. Use the ref:`topology.participant_synchronizer_states.authorize <topology.participant_synchronizer_states.authorize>` command
    to remove the participant from the synchronizer topology via the sequencer manager.

The following code snippet demonstrates the last two steps:

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/enterprise/app/src/test/scala/com/digitalasset/canton/integration/tests/offboarding/ParticipantOffboardingIntegrationTest.scala
    language: scala
    start-after: user-manual-entry-begin: OffboardParticipant
    end-before: user-manual-entry-end: OffboardParticipant
    dedent:

