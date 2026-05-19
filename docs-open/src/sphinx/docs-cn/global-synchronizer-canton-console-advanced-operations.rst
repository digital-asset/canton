..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-canton-console-advanced-operations:

Canton Network Documentation Snippets: Global Synchronizer Canton Console Advanced Operations
==============================================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_canton_console_advanced_operations
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. hidden:: val aliceParty = participant1.parties.enable("Alice")
    .. hidden:: utils.synchronize_topology()
    .. hidden:: val synchronizerId = participant1.synchronizers.list_connected().head.synchronizerId
    .. success:: participant1.topology.transactions.list()

.. snippet:: cn_global_synchronizer_canton_console_advanced_operations
    .. success:: participant1.topology.party_to_participant_mappings.list(synchronizerId)

.. snippet:: cn_global_synchronizer_canton_console_advanced_operations
    .. success:: participant1.topology.party_to_participant_mappings.list(synchronizerId = synchronizerId, filterParty = "Alice")

.. snippet:: cn_global_synchronizer_canton_console_advanced_operations
    .. hidden:: participant1.parties.disable(aliceParty)
    .. success:: val aliceParty = participant1.parties.enable("Alice")
    .. success:: participant1.topology.party_to_participant_mappings.propose(party = aliceParty, newParticipants = Seq((participant2.id, ParticipantPermission.Observation)))

.. snippet:: cn_global_synchronizer_canton_console_advanced_operations
    .. success:: participant1.keys.secret.list()
    .. success:: val newKey = participant1.keys.secret.generate_signing_key(name = "new-signing-key", usage = SigningKeyUsage.All)
    .. success:: val newEncKey = participant1.keys.secret.generate_encryption_key(name = "new-encryption-key")
