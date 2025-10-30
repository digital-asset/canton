..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _tutorial_onboard_external_multi_hosted:

===================================
Onboard Multi-Hosted External Party
===================================

This tutorial demonstrates how to onboard an **external party** using the Ledger API which is hosted on multiple validators.
It is a simple extension to the :ref:`onboard external party tutorial <tutorial_onboard_external_party_lapi>`.

Prerequisites
=============

Make sure that you have completed the :ref:`onboard external party tutorial <tutorial_onboard_external_party_lapi>`
and still have a running Canton example instance.

Run The Script
==============

The example script used in the previous tutorial also supports onboarding a multi-hosted external party. It will
onboard by default on two nodes if invoked with the ``--multi-hosted`` command line argument.

.. code-block::

    ./examples/08-interactive-submission/external_party_onboarding.sh --multi-hosted

The Details of the Script
=========================

The flag ``--multi-hosted`` will pass the second participant id into the ``generate-topology`` request through the

.. code-block::

    `"otherConfirmingParticipantUids" : [$OTHER_PARTICIPANT_ID]`

field. This will cause the generated topology transaction to include the additional participant id in the hosting
relation ship. Other options are fields such as ``observingParticipantUids``, ``confirmationThreshold`` and more.
If not configured, then the confirmation threshold will be set to the number of confirming nodes.

The generated topology transactions then just need to be uploaded to the Ledger API of the second participant:

.. literalinclude:: CANTON/community/app/src/pack/examples/08-interactive-submission/external_party_onboarding.sh
   :language: bash
   :start-after: [begin-external-party-submit-multi-hosted]
   :end-before: [end-external-party-submit-multi-hosted]

In fact, only the party to participant mapping needs to be uploaded. Uploading all topology transactions is not
necessary but harmless.

When a party to participant mapping is uploaded through the allocate endpoint which mentions the local validator,
it will automatically be signed by the local validator and forwarded to the network. If the topology transaction
is not fully authorized, which means that still some signatures are missing, it is treated as a proposal.

If the proposal already exists on the network, the new signatures are merged into the proposal and once enough
signatures are present, the topology transaction is accepted and added to the state. Because of this, the signature
of the external party can also be omitted when uploading the topology transaction to the second participant.

The hash of topology transactions is deterministic. Therefore, the same topology transaction can be created without
actually sharing the topology transaction between the different actors. The only requirement is that the content of
the transaction is the same, which at least requires knowledge of the external party's public key and the participant ids.

Distribute Topology Transactions Using the Ledger
=================================================

The described topology transaction distribution process can also be used to avoid passing the topology
transactions between the different actors for uploading to the Ledger API. Instead, using the Admin API
of the second participant, the hosting proposal can be listed, as an example, using the console command
:ref:`list_hosting_proposals <topology.party_to_participant_mappings.list_hosting_proposals>`:

You can try this out on the Canton console if you have two participants connected to the same synchronizer.
In the following example, you will use the participant1 to create the hosting proposal for an internal party.
This way, you don't need to deal with creating signatures for the topology transactions externally. The
approval of the proposal will be done using participant2.

First, create a hosting proposal using participant1:

.. snippet:: external_signing_topology_distribution
    .. hidden:: bootstrap.synchronizer_local()
    .. hidden:: participants.all.synchronizers.connect_local(sequencer1, alias = "local")
    .. success:: participant1.topology.party_to_participant_mappings.propose(
            com.digitalasset.canton.topology.PartyId.tryCreate("Alice", participant1.id.uid.namespace),
            newParticipants = Seq(
                (participant1.id, ParticipantPermission.Confirmation),
                (participant2.id, ParticipantPermission.Confirmation),
            ),
        )

Then, list the proposals on participant2. The new proposal should appear shortly:

.. snippet:: external_signing_topology_distribution
    .. hidden:: utils.retry_until_true { participant2.topology.party_to_participant_mappings.list_hosting_proposals(sequencer1.synchronizer_id, participant2.id).nonEmpty }
    .. success:: participant2.topology.party_to_participant_mappings.list_hosting_proposals(sequencer1.synchronizer_id, participant2.id)

This will show the pending proposal, awaiting the signature of the second participant. The proposal is identified
by the transaction hash ``txHash``, which can be obtained from the output of the previous command:

.. snippet:: external_signing_topology_distribution
    .. success:: val txHash = participant2.topology.party_to_participant_mappings.list_hosting_proposals(sequencer1.synchronizer_id, participant2.id).head.txHash

Authorize the proposal using the console command :ref:`topology.transactions.authorize <topology.transactions.authorize>`:

.. snippet:: external_signing_topology_distribution
    .. success:: participant2.topology.transactions.authorize(sequencer1.synchronizer_id, txHash)
    .. hidden:: utils.retry_until_true { participant1.parties.hosted("Alice").nonEmpty }

This will add the signature of participant2 to the proposal. Because the proposal is now fully signed, the party
will appear as being hosted on both nodes:

.. snippet:: external_signing_topology_distribution
    .. success:: participant1.parties.hosted("Alice")

