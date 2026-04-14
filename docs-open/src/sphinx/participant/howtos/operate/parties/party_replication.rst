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

The procedure's complexity and risk depend on whether the party has already participated
in a Daml transaction.

Therefore, you should replicate a newly onboarded party **before using it**, following
the :ref:`simple party replication<replicate-before-party-is-used>` steps.

Otherwise, you must use the :ref:`offline party replication<offline-party-replication>`
procedure.

.. note::

    **Party replication** is different from **party migration**. A party
    migration includes an additional final step: removing (or *offboarding*)
    the party from its original participant.

    Party offboarding, and thus party migration, is currently not supported.


.. _party_replication-authorization:

Party replication authorization
-------------------------------


How authorization works
^^^^^^^^^^^^^^^^^^^^^^^

Both the party and the new hosting participant must grant their consent by each issuing a
:ref:`party-to-participant mapping topology transaction<multi-hosting-authorization>`.
This ensures mutual agreement for the party replication.

External parties
^^^^^^^^^^^^^^^^

For external parties, changes to the party's topology must be explicitly authorized with
a signature of the external party's namespace key.
Whenever this guide requires party authorization, it distinguishes between *local* and
*external* parties.

When this guide uses the ``source`` participant for actions other than authorizing
topology changes, you must use one of the external party's existing confirming
participants.

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

Simple party replication consists of the following steps. You must execute them
**in order**:

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
         staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer, topologyChangeDelay = NonNegativeFiniteDuration.Zero)
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
    which is a simplification meaning Alice is registered in the participant's namespace.
    This is not a requirement.

    Alternatively, you can create the party in its own dedicated
    :externalref:`namespace<topology-namespaces>`, or create an :externalref:`external party <tutorial_onboard_external_party_lapi>`.


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
        submission permission. This allows party Alice to submit Daml transactions on it.

    .. group-tab:: External Party

        Unlike local parties who are always first hosted on
        a single node, and therefore always need to amend their party-to-participant
        mapping after the fact to be multi-hosted, external parties can do this in one
        step during the onboarding process.
        See the :externalref:`onboarding process <tutorial_onboard_external_multi_hosted>` for more details.


Authorize hosting update on the target participant
""""""""""""""""""""""""""""""""""""""""""""""""""

To complete the process, the target participant must also agree to host Alice.
Issue the **same** topology transaction on the ``target`` participant:

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
    For external parties in particular, this must be either ``Confirmation`` or
    ``Observation``.

Once the party-to-participant mapping takes effect, the replication is complete.
This results in party Alice being multi-hosted on both the ``source`` and ``target``
participants.

To replicate Alice to more participants, repeat the procedure by first vetting the
packages on a ``newTarget`` participant. Then, perform the replication again using
the original ``source`` and ``newTarget`` participants.


3.a Replicate party with simultaneous confirmation threshold change (Variant to 3)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    For **external parties**, the threshold is already defined during the
    :externalref:`onboarding process <tutorial_onboard_external_party_lapi>`,
    so this section does not apply.

To change a party's confirmation threshold, you must use a different procedure for
proposing the party-to-participant mapping than previously shown.

This alternative method allows you to perform the replication and update the threshold
in a single operation.

The following example continues from the previous one, demonstrating how to replicate
party Alice from the ``source`` participant to the ``newTarget`` participant while
simultaneously setting the confirmation threshold to three. This operation also sets
the participant permission to confirmation for all three participants that will host
Alice.

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
from a source participant and importing it to the target participant.

.. note::

    * Connect a single Canton console to both the source and target participants
      to export and import the party's ACS file using a single physical machine or
      environment. Otherwise, you must securely transfer the ACS export file
      to the target participant's environment before importing.
    * Offline party replication requires you to disconnect the target participant from all
      synchronizers before importing the party's ACS. Hence the name *offline* party replication.
    * During onboarding, you may notice ACS commitment mismatches on the target participant.
      This is expected and resolves over time; ignore these errors during the replication
      procedure.

.. warning::

    **Be advised: You must back up the target participant before you start the ACS import!**

    This ensures you have a clean recovery point if the ACS import is interrupted (crash,
    unintended node restart, etc.), or if you are otherwise unable to complete these
    manual operational steps. A backup allows you to safely reset the target participant
    and **still complete the replication**.


.. _offline-party-replication-steps:

Offline party replication steps
-------------------------------

You must perform the following steps in **the exact order** listed:

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

    You must perform offline party replication carefully and strictly follow the
    **steps in order**. Deviating from this flow will cause errors that may require
    significant manual correction.

    This documentation provides a guide. Your environment may require
    adjustments. Test thoroughly in a test environment before production use.

External parties
^^^^^^^^^^^^^^^^

For demonstration purposes, we will authorize topology transactions on behalf of
external parties using a private ED25519 key in the DER format available on disk
called ``private_key.der``. This is NOT a secure way to store private keys.
Real-world deployments must secure private keys (using a KMS for example).

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
         staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer, topologyChangeDelay = NonNegativeFiniteDuration.Zero)
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
    .. hidden:: val localAlice = alice
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

..
    Note: The following is a "macro" snippet that gets replaced with the content of allocateExternalParty.rst.macro
           It allocates an external party and makes its PartyId available in a "externalParty" value in the console.
           The whole allocation is "hidden" as in does not show up in the final RST file.

.. snippet(allocateExternalParty):: offline_party_replication

1. Vet packages
^^^^^^^^^^^^^^^

Ensure the target participant :ref:`vets all packages<package_vetting>` associated with
contracts where the party is a stakeholder.

The party ``alice`` uses the package ``CantonExamples``, which is vetted on the ``source``
participant but not yet on the ``target`` participant.

.. snippet:: offline_party_replication
    .. success:: val mainPackageId = source.dars.list(filterName = "CantonExamples").head.mainPackageId
    .. success:: target.topology.vetted_packages.list()
        .filter(_.item.packages.exists(_.packageId == mainPackageId))
        .map(r => (r.context.storeId, r.item.participantId))

Upload the missing DAR package to the ``target`` participant.

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
    Coordinate closely with other operators to ensure no external automation
    triggers pruning until the :ref:`ACS export is complete<party-replication-export-acs>`.


.. _party-replication-target-authorization:

3. Authorize new hosting on the target participant
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First, the ``target`` participant must agree to host party Alice with the desired
participant permission (*observation* in this example).

.. warning::

    Please ensure the onboarding flag is set with ``requiresPartyToBeOnboarded = true``.

.. snippet:: offline_party_replication
    .. success:: val proposal = target.topology.party_to_participant_mappings
        .propose_delta(
          party = alice,
          adds = Seq((target.id, ParticipantPermission.Observation)),
          store = synchronizerId,
          requiresPartyToBeOnboarded = true
        )
    .. hidden:: val proposal = target.topology.party_to_participant_mappings
        .propose_delta(
          party = externalParty,
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

To locate the topology transaction that authorizes the new hosting arrangement
later, record the current ledger end offset on the ``source`` participant:

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

        ..
            Note: We play a trick in this section by re-allocating "alice" to "externalAlice" in
            the console (hidden) at the beginning and re-allocating her to "localAlice" at the end.
            From the final documentation perspective it makes it look like we're acting on the same
            alice while actually running the commands on "externalAlice".

        For external parties, we need to authorize the new hosting proposal by signing the
        transaction hash with Alice's external key.

        First write the hash to a file:

        .. snippet:: offline_party_replication
            .. hidden:: val alice = externalParty
            .. success:: val tmpDir = better.files.File(s"/tmp/canton/offline_party_replication").createDirectories()
            .. success:: (tmpDir / "target_obs_topology_tx.hash").createFileIfNotExists().outputStream.apply(proposal.hash.hash.getCryptographicEvidence.writeTo(_))

        Then sign the hash. As mentioned before, we use a local private key and the ``openssl``
        command-line tool to sign the hash here for demonstration purposes. In real deployments,
        use a secure storage / signing solution.

        .. snippet:: offline_party_replication
            .. shell:: TMP_DIR=$(echo "/tmp/canton/offline_party_replication")
            .. shell:: openssl pkeyutl -sign -inkey private_key.der -rawin -in $TMP_DIR/target_obs_topology_tx.hash -out $TMP_DIR/target_obs_topology_tx.sig -keyform DER

        Finally, load the transaction with Alice's signature:

        .. snippet:: offline_party_replication
            .. success:: val aliceSignature = Signature.fromExternalSigning(
                format = SignatureFormat.Concat,
                signature = (tmpDir / "target_obs_topology_tx.sig").inputStream()(com.google.protobuf.ByteString.readFrom),
                signedBy = alice.fingerprint,
                signingAlgorithmSpec = SigningAlgorithmSpec.Ed25519,
              )
            .. success:: val proposalSignedByAlice = proposal.addSingleSignature(aliceSignature)
            .. success::
                source.topology.transactions.load(
                    transactions = Seq(proposalSignedByAlice),
                    store = synchronizerId,
                )

        .. snippet:: offline_party_replication
            .. hidden:: val alice = localAlice


.. _party-replication-export-acs:

7. Export ACS
^^^^^^^^^^^^^

Export Alice's ACS from the ``source`` participant.

The following command searches internally for the ledger offset where party Alice is
activated on the ``target`` participant, starting from ``beginOffsetExclusive``.

It then exports Alice's ACS from the ``source`` participant at that exact offset and
saves it to a file named ``party_replication.alice.acs.gz``.

.. snippet:: offline_party_replication
    .. success:: source.parties
        .export_party_acs(
          party = alice,
          synchronizerId = synchronizerId,
          targetParticipantId = target.id,
          beginOffsetExclusive = beforeActivationOffset,
          exportFilePath = "party_replication.alice.acs.gz",
        )
    .. hidden:: source.parties
        .export_party_acs(
          party = externalParty,
          synchronizerId = synchronizerId,
          targetParticipantId = target.id,
          beginOffsetExclusive = beforeActivationOffset,
          exportFilePath = "party_replication.alice_external.acs.gz",
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


.. _party-replication-import-acs:

10. Import ACS
^^^^^^^^^^^^^^

Import Alice's ACS on the ``target`` participant:

.. snippet:: offline_party_replication
    .. success:: target.parties.import_party_acs(synchronizerId, party = Some(alice), importFilePath = "party_replication.alice.acs.gz")
    .. hidden:: target.parties.import_party_acs(synchronizerId, party = Some(alice), importFilePath = "party_replication.alice_external.acs.gz")

.. note::

    Providing the party ID is optional for backward compatibility. However,
    omitting it prevents automatic onboarding flag clearance, requiring you to
    :ref:`clear the flag manually <party-replication-onboarding-flag-clearance>`.


11. Reconnect target participant to synchronizer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To later find the topology transaction that authorized the new hosting arrangement on
the ``target`` participant, record the current ledger end offset:

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
    .. hidden:: utils.retry_until_true(
         hostingParticipants.forall(_.topology.party_to_participant_mappings.is_known(
           synchronizerId,
           externalParty,
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


.. _party-replication-onboarding-flag-clearance:

13. Complete the onboarding of the party
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To complete the replication, you must clear the :ref:`previously set onboarding
flag <party-replication-target-authorization>` using the ``target`` participant.
This signals that the participant is fully ready to host the party.

If you run protocol version 35 or later and provided the party ID during the
:ref:`ACS import <party-replication-import-acs>`, this clearance is scheduled
automatically in the background upon reconnecting to the synchronizer. It
executes as soon as it is safe to do so.

.. note::

    Background flag clearance execution is observable in the participant logs.


Optional: Manual onboarding flag clearance
""""""""""""""""""""""""""""""""""""""""""

If automatic clearance does not apply, or if there are issues with the
background clearance, you must clear the flag manually.

Use the dedicated command below, which safely issues the required topology
transaction. It uses the ``targetLedgerEnd`` captured earlier to locate the
transaction that activated the party on the ``target`` participant:

.. snippet:: offline_party_replication
    .. success:: val flagStatus = target.parties
        .clear_party_onboarding_flag(alice, synchronizerId, targetLedgerEnd)
    .. assert:: flagStatus.isInstanceOf[FlagSet]
    .. hidden:: val flagStatusExternal = target.parties
        .clear_party_onboarding_flag(externalParty, synchronizerId, targetLedgerEnd)
    .. assert:: flagStatusExternal.isInstanceOf[FlagSet]

.. note::

    The ``targetLedgerEnd`` is a ledger offset on the ``target`` participant from
    where this command starts searching for the effective topology transaction that
    states that party ``alice`` is onboarding on the ``target`` participant.

The command returns the onboarding flag clearance status:

- ``FlagNotSet``: The onboarding flag is cleared.
- ``FlagSet``: The onboarding flag is still set. Removal is safe only after the indicated timestamp.

If the onboarding flag is still set, the command has internally created a
schedule to trigger the onboarding flag clearance at the appropriate time.
This happens in the background.

However, because this command is idempotent, you *may* call it repeatedly. Thus, you *may*
also poll this command until it confirms that the onboarding flag has been cleared.
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

..
    Note: We do not wait here again for the onboarding flag to be cleared on the external party to save
        on time in the generation of the snippets

.. note::

    The ``timeout`` is based on the default *decision timeout* of 1 minute.


Summary
^^^^^^^

You have successfully multi-hosted Alice on ``source`` and ``target`` participants.
