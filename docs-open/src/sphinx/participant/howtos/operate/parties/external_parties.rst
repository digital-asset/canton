..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _howto_external_parties:

.. note::
    - create a key for party namespace and party signing, or use one key for both
    - upload public keys to participant node
    - document build topology transaction for party to key mapping
    - Link to SDK site for external party prepare and execute

.. important::

    This page describes operations that can be performed by a participant operator to manage external parties hosted (or to be hosted) on their node.
    To onboard and manage external parties from the application side, refer to :ref:`this tutorial <tutorial_onboard_external_party_lapi>`.

Managing hosting relationships
==============================

Authorizing hosting of a new pending party
------------------------------------------

An essential part of the party hosting process is the participant node's approval to host the party.
This can be performed through the :subsiteref:`Ledger API <tutorial_onboard_external_party_lapi_multi_hosted_pn_approves>` with appropriate rights.
It can also be done through the node's console.

Assuming we're connected to ``participant2``'s console, list the pending parties awaiting hosting authorization from ``participant2``:

.. snippet:: external_party_authorize_hosting
    .. hidden:: val synchronizerId = bootstrap.synchronizer_local()
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "my-synchronizer")
    .. hidden:: participant2.synchronizers.connect_local(sequencer1, "my-synchronizer")
    .. hidden:: val partyId = PartyId.tryCreate("alice", participant1.fingerprint)
    .. hidden:: participant1.topology.party_to_participant_mappings.propose(
        partyId,
        Seq(
          (participant1.id, ParticipantPermission.Confirmation),
          (participant2.id, ParticipantPermission.Confirmation),
        ),
        store = synchronizerId,
      )
    .. success:: val pendingProposals = participant2.topology.party_to_participant_mappings.list_hosting_proposals(
        synchronizerId,
        participant2,
      )

We can see that Alice is to be hosted on ``participant1`` and ``participant2``.

To authorize Alice's hosting on ``participant2``:

.. snippet:: external_party_authorize_hosting
    .. success:: participant2.topology.transactions.authorize(synchronizerId, pendingProposals.head.txHash)

If both Alice and ``participant1`` already approved the hosting, we can now verify that Alice is onboarded:

.. snippet:: external_party_authorize_hosting
    .. success:: participant2.topology.party_to_participant_mappings.list(filterParty = partyId.toProtoPrimitive, synchronizerId = synchronizerId)

.. important::

    This procedure is UNSAFE if performed for an existing party. To replicate an existing party, follow the :ref:`party replication procedure <party-replication>`.