..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-multi-hosting:

Canton Network Documentation Snippets: Multi-hosting
====================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_multi_hosting
    .. hidden:: val synchronizerId = bootstrap.synchronizer_local()
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "my-synchronizer")
    .. hidden:: participant2.synchronizers.connect_local(sequencer1, "my-synchronizer")
    .. hidden:: val partyId = participant1.parties.enable("Alice")
    .. success:: participant1.topology.party_to_participant_mappings.propose(partyId, newParticipants = Seq((participant1.id, ParticipantPermission.Confirmation), (participant2.id, ParticipantPermission.Confirmation)), store = synchronizerId)

.. snippet:: cn_multi_hosting
    .. hidden:: utils.synchronize_topology()
    .. success:: val proposals = participant2.topology.party_to_participant_mappings.list_hosting_proposals(synchronizerId, participant2.id)
    .. success:: proposals.map { p => participant2.topology.transactions.authorize(synchronizerId, p.txHash); p.txHash}

.. snippet:: cn_multi_hosting
    .. success:: participant1.parties.hosted("Alice")
