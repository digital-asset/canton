..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _party-replication:

Party replication
=================

*Party replication* is the process of duplicating an existing party onto an additional
participant **within a single synchronizer**. In this process, the participant that
already hosts the party is called the *source* participant, while the new participant
is called the *target* participant.

The operational procedure differs substantially in complexity and risk depending on
whether the party you replicate has already been involved in any Daml transaction.

Therefore, onboard your party on a participant, and **before you use the party**
replicate it to other participants following the
:ref:`simple party replication<replicate-before-party-is-used>` steps.

Otherwise, you must apply an
:ref:`offline party replication<offline-party-replication>`
procedure.

.. note::

    **Party replication** is different from **party migration**. A party
    migration includes an additional final step, that is removing (or *offboarding*)
    the party from its original participant.

    Party offboarding, and thus party migration, is currently not supported.


.. _party_replication-authorization:

Party replication authorization
-------------------------------


How authorization works
^^^^^^^^^^^^^^^^^^^^^^^

Both the party and the new hosting participant must grant their consent by issuing each a
:ref:`party-to-participant mapping topology transaction<multi-hosting-authorization>`.
This ensures mutual agreement for the party replication.

External Parties
^^^^^^^^^^^^^^^^

For external parties, changes to the topology of the party must be explicitly authorized with a signature of the external party's namespace key.
Whenever in the how-to authorization from the party is required, the distinction will be made between local and external parties.
The procedure for external parties will refer to an abstract function authorizing updates to the party-to-participant mapping of the party:

.. code-block:: Python

    class HostingParticipant:
        participant_uid: str
        permission: Enums.ParticipantPermission

    def update_external_party_hosting(
        party_id: str,
        synchronizer_id: str,
        confirming_threshold: int,
        hosting_participants_add_or_update: [HostingParticipant]
    )

An example implementation of this function is given in the :externalref:`external party onboarding documentation <external_party_offline_replication>`.
The implementation additionally takes the private key of the party's namespace and a gRPC channel open to the admin API of one of the party's confirming nodes.
Those have been omitted in the function declared above for conciseness.

When the ``source`` participant is used in this how-to for actions other than authorizing topology changes,
one of the existing confirming participants of the external party must be used.

Parties with multiple owners
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When a party is owned by a group of members in a
:externalref:`decentralized namespace<decentralized-namespace>`,
a minimum number (a defined threshold) of those owners must approve the new hosting
arrangement. This threshold is met once enough individual owners each issue their own
:externalref:`party-to-participant mapping topology transaction<topology-parties>`.

Activation
^^^^^^^^^^

Completing the mutual authorization process *activates* the party on the target
participant.


.. _replicate-before-party-is-used:

Simple party replication
------------------------

The simplest and safest way to replicate a party is to do so **before** it
becomes a stakeholder in any contract.

.. warning::

    If a party has already participated in any Daml transaction, you must use
    :ref:`offline party replication<offline-party-replication>`
    instead.

The simple party replication consists of these steps, follow them **in the order**
they are listed:

#. :ref:`Create the party<party-management>`, either in the namespace of a
   participant or in a dedicated :externalref:`namespace<topology-namespaces>`.
#. :ref:`Vet packages<package_vetting>`.
#. :ref:`Authorize<party_replication-authorization>` one or more additional participants to host the party.
#. Use the party.

The following demonstrates these steps using two participants:

.. todo::
    `#27707: Remove reconciliationInterval when ACS commitments consider the onboarding flag <https://github.com/DACH-NY/canton/issues/27707>`_

.. snippet:: simple_party_replication
    .. hidden:: bootstrap.synchronizer(
         synchronizerName = "da",
         sequencers = Seq(sequencer1),
         mediators = Seq(mediator1),
         synchronizerOwners = Seq(sequencer1, mediator1),
         synchronizerThreshold = PositiveInt.one,
         staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest, topologyChangeDelay = NonNegativeFiniteDuration.Zero)
       )
    .. hidden:: sequencer1.topology.synchronizer_parameters
        .propose_update(
          sequencer1.synchronizer_id,
          _.update(
            reconciliationInterval = PositiveDurationSeconds.ofDays(365 * 10),
          )
        )
    .. hidden:: participants.all.synchronizers.connect_local(sequencer1, "mysynchronizer")
    .. success:: val source = participant1
    .. success:: val target = participant2
    .. success:: val synchronizerId = source.synchronizers.id_of("mysynchronizer")


a) Create party
^^^^^^^^^^^^^^^

:ref:`Create a party<party-management>` Alice:

.. snippet:: simple_party_replication
    .. success:: val alice = source.parties.enable("Alice", synchronizer = Some("mysynchronizer"))

.. note::

    In this example, party Alice is owned by the ``source`` participant,
    which is a simplification. It means that Alice is registered in the
    participant's namespace, but it is not a requirement.

    Alternatively, you can create the party in its own dedicated
    :externalref:`namespace<topology-namespaces>`, or create an :externalref:`external party <tutorial_onboard_external_party>`.


b) Vet packages
^^^^^^^^^^^^^^^

:ref:`Vet packages<package_vetting>` on the target participant(s) **before** proceeding.

.. note::

    If you are unfamiliar with this process, read this general explanation of
    :externalref:`package vetting<topology-package-vetting>`.


.. _multi-hosting-authorization:


c) Multi-host party
^^^^^^^^^^^^^^^^^^^

Party Alice needs to agree to be hosted on the target participant.

Because the source participant owns party Alice, you need to issue the
party-to-participant mapping topology transaction on the ``source`` participant.

Authorize hosting update on the source participant
""""""""""""""""""""""""""""""""""""""""""""""""""

.. tabs::

    .. group-tab:: Local Party

        .. snippet:: simple_party_replication
            .. success:: source.topology.party_to_participant_mappings
                .propose_delta(
                  party = alice,
                  adds = Seq(target.id -> ParticipantPermission.Submission),
                  store = synchronizerId,
                )

        :externalref:`A participant can host a party with different permissions<topology-participant-permission>`.
        In this example, the target participant will host party Alice with
        submission permission, that is party Alice can submit Daml transactions on it.

    .. group-tab:: External Party

        The :externalref:`onboarding process <external_party_onboarding_transactions>` for external parties demonstrates
        how to declare the hosting relationship of the party during the creation of the party, including hosting on multiple nodes
        (multi-hosted external party). Unlike local parties who are always first hosted on a single node, and therefore always need to amend
        their party-to-participant mapping after the fact to be multi-hosted, external parties can do this in one step during the onboarding process.
        See :externalref:`onboarding process <external_party_multi_hosting>` for more details.


Authorize hosting update on the target participant
""""""""""""""""""""""""""""""""""""""""""""""""""

To complete the process, also the target participant needs to agree to newly
host Alice. Therefore, you need to issue the **same** party-to-participant mapping
topology transaction on the ``target`` participant:

.. snippet:: simple_party_replication
    .. success:: target.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq(target.id -> ParticipantPermission.Submission),
          store = synchronizerId,
        )
    .. hidden:: utils.retry_until_true(
         source.topology.party_to_participant_mappings.is_known(
           synchronizerId,
           alice,
           Seq(source.id, target.id),
           Some(ParticipantPermission.Submission)
         )
       )

.. note::

    The participant permission here must be the same as in the previous step.
    For external parties in particular this must be either ``Confirmation`` or ``Observation``.

Once the party-to-participant mapping takes effect, the replication is complete.
This results in party Alice being multi-hosted on both the ``source`` and ``target``
participants.

To replicate Alice to more participants, repeat the procedure by first vetting the
packages on a ``newTarget`` participant. Then, perform the replication again using
the original ``source`` and ``newTarget`` participants.


c') Replicate party with simultaneous confirmation threshold change
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    For **external parties**, the threshold is defined during the :externalref:`onboarding process <tutorial_onboard_external_party>` already,
    so this section is not relevant to them.

To change a party's confirmation threshold, you must use a different procedure for
proposing the party-to-participant mapping than previously shown.

This alternative method allows you to perform the replication and update the threshold
in a single operation.

The following example continues from the previous one, demonstrating how to replicate
party Alice from the ``source`` participant to the ``newTarget`` participant while
simultaneously setting the confirmation threshold to three. This operation also sets
the participant permission to confirmation for all three participants that will be
hosting Alice.

.. snippet:: simple_party_replication
    .. success:: val newTarget = participant3
    .. success:: val hostingParticipants = Seq(source, target, newTarget)
    .. success:: source.topology.party_to_participant_mappings
        .propose(
          alice,
          newParticipants = hostingParticipants.map(_.id -> ParticipantPermission.Confirmation),
          threshold = PositiveInt.three,
          store = synchronizerId,
        )
    .. success:: newTarget.topology.party_to_participant_mappings
        .propose(
          alice,
          newParticipants = hostingParticipants.map(_.id -> ParticipantPermission.Confirmation),
          threshold = PositiveInt.three,
          store = synchronizerId,
        )
    .. hidden:: utils.retry_until_true(
        source.topology.party_to_participant_mappings.is_known(
          synchronizerId,
          alice,
          hostingParticipants.map(_.id),
          permission = Some(ParticipantPermission.Confirmation),
          threshold = Some(PositiveInt.three),
        )
    )


.. _offline-party-replication:

Offline party replication
-------------------------

Offline party replication is a :ref:`multi-step, manual process<off-pr-procedures>`.

Before replication can start, both the target participant and the party itself must
:ref:`explicitly consent to the new hosting arrangement<party_replication-authorization>`.

Afterwards, the replication consists of exporting the party's Active Contract Set (ACS)
from a source participant, and importing it to the target participant.


Operational procedures
^^^^^^^^^^^^^^^^^^^^^^

.. warning::

    Offline party replication must be performed with care, strictly following
    the documented **steps in order**. Errors can cause issues requiring manual
    correction.

    This documentation provides a guide. Your environment may require
    adjustments. Test thoroughly in a test environment before production use.

    The current offline party replication process is subject to modification in
    future releases.

This guide details the **only three supported** operational procedures for
offline party replication:

#. :ref:`Observation permission replication procedure<replicate-with-permission-change>`

    Replicate the party to the target participant with its permission set
    only to observation. You can then change this permission to either
    submission, or confirmation after specific conditions are met.

#. :ref:`Confirmation threshold replication procedure<replicate-with-threshold>`

    If it is impractical for you to change a party's permission on the target
    participant, use this method instead. You need to set the party's confirmation
    threshold to be greater than the number of concurrent replications you are
    performing.

#. :ref:`Silent synchronizer replication procedure<replicate-on-silenced-synchronizer>`

    This is your safest option for centralized, or private synchronizers. But it
    requires that you can control the synchronizer, and schedule a maintenance
    window for it.


.. _general_off_pr_steps:

Common considerations
^^^^^^^^^^^^^^^^^^^^^

All :ref:`offline party replication procedures<off-pr-procedures>` require you to
disconnect the target participant from all synchronizers before importing the
party's ACS. Hence the name *offline* party replication.

Canton's facilities for importing an ACS are only available to you when the
target participant runs in repair mode. Switching a participant's
:externalref:`repair mode<repairing-explanation>` requires a participant restart.

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/repair-commands.conf
   :language: none

While you onboard the party on the target participant you may detect ACS
commitment mismatches. This is expected and resolves itself in time;
ignore such errors during the party replication procedure.

Connect a single Canton console to both the source and target participants
to export and import the party's ACS file using a single physical machine or
environment. Otherwise, you need to securely transfer the ACS export file
to the place where you import it to the target participant.

Finally, familiarize yourself with
:externalref:`dynamic synchronizer parameters<dynamic_synchronizer_parameters>`.

.. _party-replication-decision-timeout:

The term *decision timeout*, used throughout this guide, is calculated by summing two
dynamic synchronizer parameters: ``confirmationResponseTimeout`` and
``mediatorReactionTimeout``. This timeout period guarantees that once it has elapsed,
a transaction has been fully processed by all members.

.. warning::

    Be advised, you must **back up the participant after an offline party replication**
    is complete. Restoring from a backup made before the replication causes data
    inconsistency which may remain unnoticed (a silent ledger fork) until the participant
    crashes when it attempts to exercise or archive a contract that was imported during
    the replication.



.. _off-pr-procedures:

Offline party replication procedures
------------------------------------

All procedures follow these high-level steps, which must be performed in **the exact
order** they are listed:

#. Target vetting: Ensure the target participant vets all the required packages.
#. Target participant authorizes new hosting
#. Target disconnect: The target participant disconnects from all the synchronizers.
#. Party authorizes the replication
#. ACS export: One participant already hosting the party exports the ACS.
#. ACS import: The target participant imports the ACS.
#. Target reconnect: The target participant reconnects to the synchronizers.
#. Target backup: Back up the target participant.

The actual steps may vary, and there may be more steps necessary depending
on a particular offline party replication procedure.

.. note::

    Independent on the procedure, ensure that the target participant
    :ref:`vets all packages<package_vetting>` that correspond to contracts where
    the to be replicated party is a stakeholder.


.. _replicate-with-permission-change:

Permission change replication procedure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    Only the target participant needs to run in repair mode.

This offline party replication procedure requires you to replicate the party to the
target participant with its permission set only to observation. You can then change
this permission to either submission, or confirmation after specific conditions are met.

The following demonstrates how to replicate party Alice from the ``source`` participant
to a new ``target`` participant.

.. todo::
    `#27707: Remove reconciliationInterval when ACS commitments consider the onboarding flag <https://github.com/DACH-NY/canton/issues/27707>`_

.. snippet:: offpr_permission_change_or_threshold
    .. hidden:: bootstrap.synchronizer(
         synchronizerName = "da",
         sequencers = Seq(sequencer1),
         mediators = Seq(mediator1),
         synchronizerOwners = Seq(sequencer1, mediator1),
         synchronizerThreshold = PositiveInt.one,
         staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest, topologyChangeDelay = NonNegativeFiniteDuration.Zero)
       )
    .. hidden:: sequencer1.topology.synchronizer_parameters
        .propose_update(
          sequencer1.synchronizer_id,
          _.update(
            reconciliationInterval = PositiveDurationSeconds.ofDays(365 * 10),
          )
        )
    .. success:: val source = participant1
    .. success:: val target = participant2
    .. hidden:: participants.all.synchronizers.connect_local(sequencer1, "mysynchronizer")
    .. success:: val alice = source.parties.enable("Alice", synchronizer = Some("mysynchronizer")) // This command creates a local party. For external parties see the external party onboarding documentation (link found above in this page)
    .. hidden:: source.ledger_api.javaapi.commands.submit(
         Seq(alice),
         new Ping(
           "hello",
           alice.toProtoPrimitive,
           alice.toProtoPrimitive
         ).create.commands.asScala.toSeq,
       )
    .. success:: val synchronizerId = source.synchronizers.id_of("mysynchronizer")


a) Authorize new hosting on the target participant
""""""""""""""""""""""""""""""""""""""""""""""""""

First, have the ``target`` participant to agree to host party Alice:

.. snippet:: offpr_permission_change_or_threshold
    .. success:: target.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq((target.id, ParticipantPermission.Observation)),
          store = synchronizerId,
          requiresPartyToBeOnboarded = true
        )

b) Disconnect target participant from synchronizers
"""""""""""""""""""""""""""""""""""""""""""""""""""

.. snippet:: offpr_permission_change_or_threshold
    .. success:: target.synchronizers.disconnect_all()


c) Authorize new hosting for the party
""""""""""""""""""""""""""""""""""""""

As you will need to find the ledger offset of the topology transaction which
authorizes the new hosting arrangement, take the current ledger end offset:

.. snippet:: offpr_permission_change_or_threshold
    .. success:: val beforeActivationOffset = source.ledger_api.state.end()

Only after the target participant has been disconnected from all synchronizers,
have party Alice agree to be hosted on it.

.. tabs::

    .. group-tab:: Local Party

        .. snippet:: offpr_permission_change_or_threshold
            .. success:: source.topology.party_to_participant_mappings
                .propose_delta(
                  party = alice,
                  adds = Seq((target.id, ParticipantPermission.Observation)),
                  store = synchronizerId,
                  requiresPartyToBeOnboarded = true
                )

    .. group-tab:: External Party

        .. code-block:: Python

            update_external_party_hosting(
                party_id = alice,
                synchronizer_id = synchronizerId,
                confirming_threshold = None, # Keep current threshold
                hosting_participants_add_or_update: [
                    HostingParticipant(participant_uid = target.id, ParticipantPermission.Observation, onboarding = HostingParticipant.Onboarding())
                ]
            )


d) Export ACS
"""""""""""""

Export Alice's ACS from the ``source`` participant.

The following command finds the ledger offset where party Alice is activated on
the ``target`` participant, starting the search from ``beginOffsetExclusive``.
It then exports Alice's ACS from the ``source`` participant at that exact offset,
and stores it in the export file named ``party_replication.alice.acs.gz``.

.. snippet:: offpr_permission_change_or_threshold
    .. success:: source.parties
        .export_party_acs(
          party = alice,
          synchronizerId = synchronizerId,
          targetParticipantId = target.id,
          beginOffsetExclusive = beforeActivationOffset,
          exportFilePath = "party_replication.alice.acs.gz",
        )


e) Import ACS
"""""""""""""

Import Alice's ACS in the ``target`` participant:

.. snippet:: offpr_permission_change_or_threshold
    .. success:: target.parties.import_party_acs("party_replication.alice.acs.gz")


f) Reconnect target participant to synchronizer
"""""""""""""""""""""""""""""""""""""""""""""""

.. snippet:: offpr_permission_change_or_threshold
    .. success:: target.synchronizers.reconnect_local("mysynchronizer")
    .. hidden:: val hostingParticipants = Seq(source, target)
    .. hidden:: utils.retry_until_true(
         hostingParticipants.forall(_.topology.party_to_participant_mappings.is_known(
           synchronizerId,
           alice,
           Seq(target.id),
           Some(ParticipantPermission.Observation)
         )
       ))

g) Clear the participant's onboarding flag
""""""""""""""""""""""""""""""""""""""""""

After the ``target`` participant has completed the ACS import and has reconnected to the synchronizer,
it must clear the onboarding flag, to signal to the party that it is now ready to assume the responsibilities of
a hosting participant.

.. snippet:: offpr_permission_change_or_threshold
    .. success:: target.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq((target.id, ParticipantPermission.Observation)),
          store = synchronizerId,
        )
    .. hidden:: val hostingParticipants = Seq(source, target)
    .. hidden:: utils.retry_until_true(
         hostingParticipants.forall(_.topology.party_to_participant_mappings.is_known(
           synchronizerId,
           alice,
           Seq(target.id),
           Some(ParticipantPermission.Observation)
         )
       ))

h) Change participant permission
""""""""""""""""""""""""""""""""

After waiting for at least the :ref:`decision timeout<party-replication-decision-timeout>`
to elapse, set the participant's permission from ``Observation`` to ``Submission`` (for local parties), or
``Confirmation`` depending on your particular needs.

.. tabs::

    .. group-tab:: Local Party

        The following sets Alice's permission to the ``Submission`` participant permission:

        .. snippet:: offpr_permission_change_or_threshold
            .. success:: Seq(source, target).foreach(
                 _.topology.party_to_participant_mappings .propose_delta(
                   party = alice,
                   adds = Seq((target.id, ParticipantPermission.Submission)),
                   store = synchronizerId,
                 )
               )
            .. hidden:: val hostingParticipants = Seq(source, target)
            .. hidden:: utils.retry_until_true(
                 target.topology.party_to_participant_mappings.is_known(
                   synchronizerId,
                   alice,
                   hostingParticipants.map(_.id).toSeq,
                   Some(ParticipantPermission.Submission)
                 )
               )
    .. hidden:: target.ledger_api.javaapi.commands.submit(
                 Seq(alice),
                 new Ping(
                   "hello",
                   alice.toProtoPrimitive,
                   alice.toProtoPrimitive
                 ).create.commands.asScala.toSeq,
               )

    .. group-tab:: External Party

        .. code-block:: Python

            update_external_party_hosting(
                party_id = alice,
                synchronizer_id = synchronizerId,
                confirming_threshold = None, # Keep current threshold
                hosting_participants_add_or_update: [
                    HostingParticipant(participant_uid = target.id, ParticipantPermission.Confirmation)
                ]
            )

.. note::

    It may be surprising that the target participant does not authorize the permission
    change. Remember that a
    :externalref:`participant permission has be assigned by the party's owner<topology-parties>`,
    and party Alice is owned by the ``source`` participant, in this example.


i) Back up participant
""""""""""""""""""""""

**Back up the target participant!**

Restoring from a backup made before the replication causes unnoticeable data
inconsistency until the participant crashes when it attempts to exercise or
archive a contract that was imported during the replication.


j) Summary
""""""""""

You have successfully multi-hosted Alice on ``source`` and ``target`` participants
with ``Submission`` permission. With a confirmation threshold of one, you can now
use Alice in Daml transactions originating from either participant.


.. _replicate-with-threshold:

Threshold change replication procedure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    Only the target participant needs to run in repair mode.

Unlike the
:ref:`previous procedure<replicate-with-permission-change>`,
this offline party replication procedure avoids later permission changes, requiring
you to set the party's confirmation threshold.

The following example shows the replication of party Alice, already multi-hosted
on ``participant1`` and ``participant2``, to an additional ``newTarget`` participant.

.. snippet:: offpr_permission_change_or_threshold
    .. success:: val hostingParticipants = Seq(participant1, participant2)
    .. success:: val newTarget = participant3


a) Set confirmation threshold of multi-hosted party
"""""""""""""""""""""""""""""""""""""""""""""""""""

.. note::

    For safe party replication, always set the confirmation threshold higher
    than the number of concurrent replications.

Set the confirmation threshold of party Alice to two:

.. tabs::

    .. group-tab:: Local Party

        .. snippet:: offpr_permission_change_or_threshold
            .. success:: source.topology.party_to_participant_mappings
                .propose(
                  alice,
                  newParticipants = hostingParticipants.map(_.id -> ParticipantPermission.Submission),
                  threshold = PositiveInt.two,
                  store = synchronizerId,
                )
            .. hidden:: utils.retry_until_true(
                source.topology.party_to_participant_mappings.is_known(
                  synchronizerId,
                  alice,
                  hostingParticipants.map(_.id),
                  threshold = Some(PositiveInt.two),
                )
            )

    .. group-tab:: External Party

        .. code-block:: Python

            update_external_party_hosting(
                party_id = alice,
                synchronizer_id = synchronizerId,
                confirming_threshold = 2,
                hosting_participants_add_or_update: []
            )


b) Authorize new hosting on the target participant
""""""""""""""""""""""""""""""""""""""""""""""""""

.. snippet:: offpr_permission_change_or_threshold
    .. success:: newTarget.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq((newTarget.id, ParticipantPermission.Submission)),
          store = synchronizerId,
          requiresPartyToBeOnboarded = true
        )

c) Disconnect target participant
""""""""""""""""""""""""""""""""

.. snippet:: offpr_permission_change_or_threshold
    .. success:: newTarget.synchronizers.disconnect_all()


d) Authorize new hosting for the party
""""""""""""""""""""""""""""""""""""""

Ensure the ``newTarget`` participant has been disconnected from all synchronizers.

Once confirmed, take the current ledger end offset, as this value will be needed
later.

.. snippet:: offpr_permission_change_or_threshold
    .. success:: val newBeforeActivationOffset = source.ledger_api.state.end()

Finally, have party Alice agree to be hosted on the ``newTarget`` participant.

.. tabs::

    .. group-tab:: Local Party

        .. snippet:: offpr_permission_change_or_threshold
            .. success:: source.topology.party_to_participant_mappings
                .propose_delta(
                  party = alice,
                  adds = Seq((newTarget.id, ParticipantPermission.Submission)),
                  store = synchronizerId,
                  requiresPartyToBeOnboarded = true
                )

    .. group-tab:: External Party

        .. code-block:: Python

            update_external_party_hosting(
                party_id = alice,
                synchronizer_id = synchronizerId,
                confirming_threshold = None, # Keep current threshold
                hosting_participants_add_or_update: [
                    HostingParticipant(participant_uid = newTarget.id, ParticipantPermission.Confirmation, onboarding = HostingParticipant.Onboarding())
                ]
            )

e) Export ACS
"""""""""""""

Export Alice's ACS from the ``source`` participant.

Following command finds the ledger offset where party Alice is activated on
the ``newTarget`` participant, starting the search from ``newBeforeActivationOffset``.
It then exports Alice's ACS from the ``source`` participant at that exact offset,
and stores it in the export file named ``party_replication.alice.acs.gz``.

.. snippet:: offpr_permission_change_or_threshold
    .. success:: source.parties
        .export_party_acs(
          party = alice,
          synchronizerId = synchronizerId,
          targetParticipantId = newTarget.id,
          beginOffsetExclusive = newBeforeActivationOffset,
          exportFilePath = "party_replication.alice.acs.gz",
        )


f) Import ACS
"""""""""""""

Import Alice's ACS in the ``newTarget`` participant.

.. snippet:: offpr_permission_change_or_threshold
    .. success:: newTarget.repair.import_acs("party_replication.alice.acs.gz")


g) Reconnect target participant to synchronizer
"""""""""""""""""""""""""""""""""""""""""""""""

.. snippet:: offpr_permission_change_or_threshold
    .. success:: newTarget.synchronizers.reconnect_local("mysynchronizer")


h) Clear the participant's onboarding flag
""""""""""""""""""""""""""""""""""""""""""

After the ``newTarget`` participant has completed the ACS import and has reconnected to the synchronizer,
it must clear the onboarding flag, to signal to the party that it is now ready to assume the responsibilities of
a hosting participant.

.. snippet:: offpr_permission_change_or_threshold
    .. success:: newTarget.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq((newTarget.id, ParticipantPermission.Submission)),
          store = synchronizerId,
        )
    .. hidden:: val hostingParticipants = Seq(source, target, newTarget)
    .. hidden:: utils.retry_until_true(
         hostingParticipants.forall(_.topology.party_to_participant_mappings.is_known(
           synchronizerId,
           alice,
           hostingParticipants.map(_.id),
           Some(ParticipantPermission.Submission)
         )
       ))

i) Back up participant
""""""""""""""""""""""

**Back up the target participant!**

Restoring from a backup made before the replication causes unnoticeable data
inconsistency until the participant crashes when it attempts to exercise or
archive a contract that was imported during the replication.


j) Summary
""""""""""

You have successfully multi-hosted party Alice across three participants with
``Submission`` permission. The confirmation threshold of two prevents her from
submitting Daml transactions, though she can still submit reassignments.


.. _replicate-on-silenced-synchronizer:

Silent synchronizer replication procedure
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::
    Silent synchronizer replication has **significant operational restrictions**.
    It requires running both the source and target participants in repair mode,
    and :ref:`silencing the synchronizer<party-replication-silence-synchronizer>`,
    so it must be done during a maintenance window.

    This procedure is only suitable for a synchronizer that is fully controlled by
    a single operational entity. For example, this method cannot be used for a
    synchronizer like the Global Synchronizer as it is impractical.

This guide demonstrates how to replicate a party Alice from the ``source`` participant
to the ``target`` participant, using a synchronizer with the alias ``mysynchronizer``.
For simplicity's sake, party Alice is owned by the ``source`` participant.

.. todo::
    `#27707: Remove reconciliationInterval when ACS commitments consider the onboarding flag <https://github.com/DACH-NY/canton/issues/27707>`_

.. snippet:: offpr_silent_synchronizer
    .. hidden:: bootstrap.synchronizer(
         synchronizerName = "da",
         sequencers = Seq(sequencer1),
         mediators = Seq(mediator1),
         synchronizerOwners = Seq(sequencer1, mediator1),
         synchronizerThreshold = PositiveInt.one,
         staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest, topologyChangeDelay = NonNegativeFiniteDuration.Zero)
       )
    .. hidden:: val loweredTimeout = NonNegativeFiniteDuration.ofSeconds(5)
    .. hidden:: val customMediatorReactionTimeout = loweredTimeout
    .. hidden:: val customConfirmationResponseTimeout = loweredTimeout
    .. hidden:: sequencer1.topology.synchronizer_parameters
        .propose_update(
          sequencer1.synchronizer_id,
          _.update(
            confirmationResponseTimeout = customConfirmationResponseTimeout,
            mediatorReactionTimeout = customMediatorReactionTimeout,
            reconciliationInterval = PositiveDurationSeconds.ofDays(365 * 10),
          )
        )
    .. success:: val source = participant1
    .. success:: val target = participant2
    .. hidden:: participants.all.synchronizers.connect_local(sequencer1, "mysynchronizer")
    .. success:: val alice = source.parties.enable("Alice", synchronizer = Some("mysynchronizer"))
    .. hidden:: source.ledger_api.javaapi.commands.submit(
         Seq(alice),
         new Ping(
           "hello",
           alice.toProtoPrimitive,
           alice.toProtoPrimitive
         ).create.commands.asScala.toSeq,
       )
    .. success:: val synchronizerId = source.synchronizers.id_of("mysynchronizer")


.. _party-replication-silence-synchronizer:

a) Silence synchronizer
"""""""""""""""""""""""

First, propose an update to silence the synchronizer by setting its
``confirmationRequestsMaxRate`` to zero using the command below. After the command
succeeds, you must wait for at least the
:ref:`decision timeout<party-replication-decision-timeout>`
to elapse before continuing.

.. snippet:: offpr_silent_synchronizer
    .. success:: sequencer1.topology.synchronizer_parameters
        .propose_update(
          sequencer1.synchronizer_id,
          _.update(
            confirmationRequestsMaxRate = NonNegativeInt.zero,
          )
        )

.. warning::

    Failure to silence the synchronizer leads to data corruption and ledger forks.


b) Authorize new hosting
""""""""""""""""""""""""

For the new hosting arrangement to proceed, both party Alice and the ``target``
participant must consent via topology transactions.

First, capture the current ledger end offset. You will need this ``beforeActivationOffset``
value later.

.. snippet:: offpr_silent_synchronizer
    .. success:: val beforeActivationOffset = source.ledger_api.state.end()

Next, establish mutual consent.

.. tabs::

    .. group-tab:: Local Party

        The ``source`` participant proposes a transaction on behalf of Alice (Alice is owned by the source participant).

        .. snippet:: offpr_silent_synchronizer
            .. success:: source.topology.party_to_participant_mappings
                .propose_delta(
                  party = alice,
                  adds = Seq((target.id, ParticipantPermission.Observation)),
                  store = synchronizerId,
                  requiresPartyToBeOnboarded = true
                )

    .. group-tab:: External Party

        .. code-block:: Python

            update_external_party_hosting(
                party_id = alice,
                synchronizer_id = synchronizerId,
                confirming_threshold = None, # Keep current threshold
                hosting_participants_add_or_update: [
                    HostingParticipant(participant_uid = target.id, ParticipantPermission.Observation, onboarding=HostingParticipant.Onboarding())
                ]
            )

The ``target`` participant proposes one for itself.

.. snippet:: offpr_silent_synchronizer
    .. success:: target.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq((target.id, ParticipantPermission.Observation)),
          store = synchronizerId,
          requiresPartyToBeOnboarded = true
        )

This completes the authorization.

c) Export ACS
"""""""""""""

Export Alice's ACS from the ``source`` participant at the specific ledger offset
when the new hosting arrangement for Alice has become effective.

The following repair macro, ``step1_hold_and_store_acs``, uses the ``beforeActivationOffset``
value you captured earlier as the starting point for finding the correct ledger offset
for the export.

.. snippet:: offpr_silent_synchronizer
    .. hidden:: val bufferTime = NonNegativeFiniteDuration.ofMillis(100)
    .. hidden:: val waitTime = customMediatorReactionTimeout + customConfirmationResponseTimeout + bufferTime
    .. hidden:: com.digitalasset.canton.concurrent.Threading.sleep(waitTime.duration.toMillis)
    .. success:: repair.party_replication.step1_hold_and_store_acs(
        partyId = alice,
        synchronizerId = synchronizerId,
        sourceParticipant = source,
        targetParticipantId = target,
        targetFile = "party_replication.alice.acs.gz",
        beginOffsetExclusive = beforeActivationOffset,
      )

This stores Alice's ACS in the export file named ``party_replication.alice.acs.gz``.


d) Import ACS
"""""""""""""

Import Alice's ACS in the ``target`` participant.

.. snippet:: offpr_silent_synchronizer
    .. success:: repair.party_replication.step2_import_acs(
        partyId = alice,
        synchronizerId = synchronizerId,
        targetParticipant = target,
        sourceFile = "party_replication.alice.acs.gz"
      )


e) Resume synchronizer
""""""""""""""""""""""

Resume the synchronizer by restoring the confirmation request maximum rate to
a positive value.

.. snippet:: offpr_silent_synchronizer
    .. success:: sequencer1.topology.synchronizer_parameters.propose_update(
        sequencer1.synchronizer_id,
        _.update(
          confirmationRequestsMaxRate = NonNegativeInt.tryCreate(10000)
        )
      )
    .. hidden:: source.health.ping(target)

f) Clear the participant's onboarding flag
""""""""""""""""""""""""""""""""""""""""""

After the ``target`` participant has completed the ACS import and has reconnected to the synchronizer,
it must clear the onboarding flag, to signal to the party that it is now ready to assume the responsibilities of
a hosting participant.

.. snippet:: offpr_silent_synchronizer
    .. success:: target.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq((target.id, ParticipantPermission.Observation)),
          store = synchronizerId,
        )
    .. hidden:: val hostingParticipants = Seq(source, target)
    .. hidden:: utils.retry_until_true(
         hostingParticipants.forall(_.topology.party_to_participant_mappings.is_known(
           synchronizerId,
           alice,
           Seq(target.id),
           Some(ParticipantPermission.Observation)
         )
       ))


g) Adjust participant permission
""""""""""""""""""""""""""""""""

Once at least the
:ref:`decision timeout<party-replication-decision-timeout>`
has passed, you may change the party's permission from ``Observation`` to
``Submission``, or ``Confirmation``, as required.

The command below sets Alice's permission to ``Submission`` on the ``target`` participant.

.. tabs::

    .. group-tab:: Local Party

        .. snippet:: offpr_silent_synchronizer
            .. success:: Seq(source, target).foreach(
                 _.topology.party_to_participant_mappings .propose_delta(
                   party = alice,
                   adds = Seq((target.id, ParticipantPermission.Submission)),
                   store = synchronizerId,
                 )
               )
            .. hidden:: val hostingParticipants = Seq(source, target)
            .. hidden:: utils.retry_until_true(
                 target.topology.party_to_participant_mappings.is_known(
                   synchronizerId,
                   alice,
                   hostingParticipants.map(_.id).toSeq,
                   Some(ParticipantPermission.Submission)
                 )
               )
    .. hidden:: target.ledger_api.javaapi.commands.submit(
                 Seq(alice),
                 new Ping(
                   "hello",
                   alice.toProtoPrimitive,
                   alice.toProtoPrimitive
                 ).create.commands.asScala.toSeq,
               )

    .. group-tab:: External Party

        .. code-block:: Python

            update_external_party_hosting(
                party_id = alice,
                synchronizer_id = synchronizerId,
                confirming_threshold = None, #Keep current threshold
                hosting_participants_add_or_update: [
                    HostingParticipant(participant_uid = target.id, ParticipantPermission.Confirmation)
                ]
            )


h) Back up participant
""""""""""""""""""""""

**Back up the target participant!**

Restoring from a backup made before the replication causes unnoticeable data
inconsistency until the participant crashes when it attempts to exercise or
archive a contract that was imported during the replication.


i) Summary
""""""""""

You have successfully replicated Alice. She is now multi-hosted on both the
``source``, and ``target`` participants with ``Submission`` permission. With a
confirmation threshold of one, you can now use Alice in Daml transactions
originating from either participant.
