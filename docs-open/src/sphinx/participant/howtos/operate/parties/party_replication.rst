..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

External parties
^^^^^^^^^^^^^^^^

For external parties, changes to the party's topology must be explicitly authorized with
a signature of the external party's namespace key.
Whenever in the how-to authorization from the party is required, the distinction will be
made between *local* and *external* parties.
The procedure for external parties will refer to an abstract function authorizing updates
to the party's party-to-participant mapping:

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

An example implementation of this function is given in the
:externalref:`external party onboarding documentation <external_party_offline_replication>`.
The implementation additionally takes the private key of the party's namespace and a gRPC
channel connected to the admin API of one of the party's confirming nodes.
Those have been omitted in the function declared above for conciseness.

When the ``source`` participant is used in this how-to for actions other than authorizing
topology changes, one of the existing confirming participants of the external party must be used.

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


1. Create party
^^^^^^^^^^^^^^^

:ref:`Create a party<party-management>` Alice:

.. snippet:: simple_party_replication
    .. success:: val alice = source.parties.enable("Alice", synchronizer = Some("mysynchronizer"))

.. note::

    In this example, the **local party Alice** is owned by the ``source`` participant,
    which is a simplification. It means that Alice is registered in the participant's
    namespace, but it is not a requirement.

    Alternatively, you can create the party in its own dedicated
    :externalref:`namespace<topology-namespaces>`, or create an :externalref:`external party <tutorial_onboard_external_party>`.


2. Vet packages
^^^^^^^^^^^^^^^

:ref:`Vet packages<package_vetting>` on the target participant(s) **before** proceeding.

.. note::

    If you are unfamiliar with this process, read this general explanation of
    :externalref:`package vetting<topology-package-vetting>`.


.. _multi-hosting-authorization:


3. Multi-host party
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

        The :externalref:`onboarding process <external_party_onboarding_transactions>`
        for external parties demonstrates how to declare the hosting relationship of the
        party during the creation of the party, including hosting on multiple nodes
        (multi-hosted external party). Unlike local parties who are always first hosted on
        a single node, and therefore always need to amend their party-to-participant
        mapping after the fact to be multi-hosted, external parties can do this in one
        step during the onboarding process.
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


3.a Replicate party with simultaneous confirmation threshold change (Variant to 3)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

Offline party replication is a :ref:`multi-step, manual process<offline-party-replication-steps>`.

Before replication can start, both the target participant and the party itself must
:ref:`explicitly consent to the new hosting arrangement<party_replication-authorization>`.

Afterwards, the replication consists of exporting the party's Active Contract Set (ACS)
from a source participant, and importing it to the target participant.

.. note::

    * Connect a single Canton console to both the source and target participants
      to export and import the party's ACS file using a single physical machine or
      environment. Otherwise, you need to securely transfer the ACS export file
      to the place where you import it to the target participant.
    * Offline party replication requires you to disconnect the target participant from all
      synchronizers before importing the party's ACS. Hence the name *offline* party replication.
    * While you onboard the party on the target participant you may detect ACS
      commitment mismatches. This is expected and resolves itself in time; ignore such errors
      during the party replication procedure.

.. warning::

    **Be advised: You must back up the target participant before you start the ACS import!**

    This ensures you have a clean recovery point if the ACS import is interrupted (crash,
    unintended node restart, etc.), or when you otherwise were unable to follow this manual
    operational steps to completion. Having this backup allows you to safely reset the target
    participant and **still complete the ongoing offline party replication**.


.. _offline-party-replication-steps:

Offline party replication steps
-------------------------------

These are the steps, which you must perform in **the exact order** they are listed:

#. **Target: Package Vetting** – Ensure the target participant vets all required packages.
#. **Source: Data Retention** - Ensure the source participant retains data long enough for the export.
#. **Target: Authorization** - Target participant authorizes new hosting with the onboarding flag set.
#. **Target: Isolation** - Disconnect from all synchronizers and disable auto-reconnect upon restart.
#. **Source: Party Authorization** - Party authorizes the replication with the onboarding flag set.
#. **Source: ACS Export** - The participant currently hosting the party exports the ACS.
#. **Target: Backup** - Back up the target participant before starting the ACS import.
#. **Target: ACS Import** - The target participant imports the ACS.
#. **Target: Reconnect** - The target participant reconnects to the synchronizers.
#. **Target: Onboarding Flag Clearance** - The target participant issues the onboarding flag clearance.

.. warning::

    Offline party replication must be performed with care, strictly following
    the documented **steps in order**. Not following the outlined operational
    flow will result in errors potentially requiring significant manual correction.

    This documentation provides a guide. Your environment may require
    adjustments. Test thoroughly in a test environment before production use.


Scenario description
^^^^^^^^^^^^^^^^^^^^

The following steps show how to replicate party ``alice`` from the ``source``
participant to a new ``target`` participant on the synchronizer ``mysynchronizer``.
The ``source`` can be any participant already hosting the party.

.. todo::
    `#27707: Remove reconciliationInterval when ACS commitments consider the onboarding flag <https://github.com/DACH-NY/canton/issues/27707>`_

.. snippet:: offline_party_replication
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
    .. hidden:: source.pruning.set_schedule("0 0 20 * * ?", 2.hours, 30.days)
    .. hidden:: source.dars.upload("dars/CantonExamples.dar")


1. Vet packages
^^^^^^^^^^^^^^^

Ensure the target participant :ref:`vets all packages<package_vetting>` associated with
contracts where the party is a stakeholder.

The party ``alice`` uses the package ``CantonExamples`` which is vetted on the ``source``
participant but not yet on the ``target`` participant.

.. snippet:: offline_party_replication
    .. success:: val mainPackageId = source.dars.list(filterName = "CantonExamples").head.mainPackageId
    .. success:: target.topology.vetted_packages.list()
        .filter(_.item.packages.exists(_.packageId == mainPackageId))
        .map(r => (r.context.storeId, r.item.participantId))

Hence, upload the missing DAR package to the ``target`` participant.

.. snippet:: offline_party_replication
    .. success:: target.dars.upload("dars/CantonExamples.dar")
    .. success:: target.topology.vetted_packages.list()
        .filter(_.item.packages.exists(_.packageId == mainPackageId))
        .map(r => (r.context.storeId, r.item.participantId))


.. _party-replication-data-retention:

2. Data Retention
^^^^^^^^^^^^^^^^^

Ensure that the retention period on the source participant is long enough to cover the
entire duration between the following two events:

#. The :ref:`party-to-participant mapping topology transaction becoming effective<party-replication-complete-authorization>`.
#. The completion of the :ref:`ACS export from the source participant<party-replication-export-acs>`.

If you are unsure whether the current retention period is sufficient, or as an additional precaution,
you should temporarily disable :externalref:`automatic pruning<participant-node-pruning-howto>`
on the source participant.

Retrieve the current automatic pruning schedule. This command returns ``None`` if no
schedule is set.

.. snippet:: offline_party_replication
    .. success:: val pruningSchedule = source.pruning.get_schedule()


Clear the pruning schedule, disabling the automatic pruning on the ``source`` node.

.. snippet:: offline_party_replication
    .. success:: source.pruning.clear_schedule()


.. warning::

    Manual pruning cannot be programmatically disabled on the ``source`` participant.
    Coordinate closely with other operators and ensure that no external automation
    triggers pruning until the :ref:`ACS export is complete<party-replication-export-acs>`.


3. Authorize new hosting on the target participant
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First, have the ``target`` participant agree to host party Alice with the desired
participant permission (*observation* in this example).

.. warning::

    Please ensure the onboarding flag is set with ``requiresPartyToBeOnboarded = true``.

.. snippet:: offline_party_replication
    .. success:: target.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq((target.id, ParticipantPermission.Observation)),
          store = synchronizerId,
          requiresPartyToBeOnboarded = true
        )


4. Disconnect target participant from all synchronizers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. snippet:: offline_party_replication
    .. success:: target.synchronizers.disconnect_all()


.. _party-replication-disable-auto-reconnect:

5. Disable auto-reconnect on target participant
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ensure the target participant does not automatically reconnect to the synchronizer upon restart.

.. snippet:: offline_party_replication
    .. success:: target.synchronizers.config("mysynchronizer")
    .. success:: target.synchronizers.modify("mysynchronizer", _.copy(manualConnect=true))
    .. success:: target.synchronizers.config("mysynchronizer")


.. _party-replication-complete-authorization:

6. Authorize new hosting for the party
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To later *find* the ledger offset of the topology transaction which authorizes the new
hosting arrangement, take the current ledger end offset on the ``source`` participant
as a starting point:

.. snippet:: offline_party_replication
    .. success:: val beforeActivationOffset = source.ledger_api.state.end()

**Only after** the target participant has been disconnected from all synchronizers,
have party Alice agree to be hosted on it.

.. warning::

    Again, please ensure the onboarding flag is set with ``requiresPartyToBeOnboarded = true``
    for a local party, and with ``onboarding = HostingParticipant.Onboarding()`` for external party.

.. tabs::

    .. group-tab:: Local Party

        .. snippet:: offline_party_replication
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


.. _party-replication-export-acs:

7. Export ACS
^^^^^^^^^^^^^

Export Alice's ACS from the ``source`` participant.

The following command finds internally the ledger offset where party Alice is activated on
the ``target`` participant, starting the search from ``beginOffsetExclusive``.

It then exports Alice's ACS from the ``source`` participant at that exact offset, and stores
it in the export file named ``party_replication.alice.acs.gz``.

.. snippet:: offline_party_replication
    .. success:: source.parties
        .export_party_acs(
          party = alice,
          synchronizerId = synchronizerId,
          targetParticipantId = target.id,
          beginOffsetExclusive = beforeActivationOffset,
          exportFilePath = "party_replication.alice.acs.gz",
        )


8. Optional: Re-enable automatic pruning
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you previously disabled automatic pruning on the ``source`` participant by following
the :ref:`data retention step<party-replication-data-retention>`,
you may now re-enable it.

Run the following command using the original configuration parameters you recorded before disabling the schedule:

.. snippet:: offline_party_replication
    .. success:: source.pruning.set_schedule("0 0 20 * * ?", 2.hours, 30.days)


9. Back up target participant
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning:: **Please back up the target participant before importing the ACS!**


10. Import ACS
^^^^^^^^^^^^^^

Import Alice's ACS in the ``target`` participant:

.. snippet:: offline_party_replication
    .. success:: target.parties.import_party_acs("party_replication.alice.acs.gz")


11. Reconnect target participant to synchronizer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To later *find* the ledger offset of the topology transaction where the new hosting
arrangement on the ``target`` participant has been authorized, take the current ledger
end offset:

.. snippet:: offline_party_replication
    .. success:: val targetLedgerEnd = target.ledger_api.state.end()


Now, reconnect that ``target`` participant to the synchronizer.

.. snippet:: offline_party_replication
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


12. Optional: Re-enable auto-reconnect on target participant
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you previously disabled auto-reconnect following the :ref:`earlier step<party-replication-disable-auto-reconnect>`,
you may now re-enable it. This is only necessary if the target participant was originally
configured to reconnect automatically upon restart.

.. snippet:: offline_party_replication
    .. success:: target.synchronizers.modify("mysynchronizer", _.copy(manualConnect=false))


13. Clear the participant's onboarding flag
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After the ``target`` participant has completed the ACS import and reconnected to the
synchronizer, you must clear the onboarding flag. This signals that the participant
is fully ready to host the party.

There is a dedicated command to accomplish the onboarding flag clearance. It will issue
the topology transaction to clear the flag for you, but only when it is safe to do so.

The following command uses the ``targetLedgerEnd`` captured in the previous step as the
starting point to internally locate the effective party-to-participant mapping transaction
that has activated ``alice`` on the ``target`` participant.

.. snippet:: offline_party_replication
    .. success:: val flagStatus = target.parties
        .clear_party_onboarding_flag(alice, synchronizerId, targetLedgerEnd)
    .. assert:: flagStatus.isInstanceOf[FlagSet]

The command returns the onboarding flag clearance status:

- ``FlagNotSet``: The onboarding flag is cleared. Proceed to the next step.
- ``FlagSet``: The onboarding flag is still set. Removal is safe
  only after the indicated timestamp.

If the onboarding flag is still set and you have called this command for the first time,
it has internally created a schedule to trigger the onboarding flag clearance at the
appropriate time. This happens in the background.

However, because this command is idempotent, you *may* call it repeatedly. Thus, you
*may* also poll this command until it confirms that the onboarding flag has been cleared.
The following snippet demonstrates how this command can be polled.

.. snippet:: offline_party_replication
    .. success:: utils.retry_until_true(timeout = 2.minutes, maxWaitPeriod = 1.minutes) {
          val flagStatus = target.parties
            .clear_party_onboarding_flag(alice, synchronizerId, targetLedgerEnd)
          flagStatus match {
           case FlagSet(_) => false
           case FlagNotSet => true
          }
        }

.. note::

    The ``timeout`` is based on the default *decision timeout* of 1 minute.


Summary
^^^^^^^

You have successfully multi-hosted Alice on ``source`` and ``target`` participants.
