..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-canton-console-scripting:

Canton Network Documentation Snippets: Global Synchronizer Canton Console Scripting
=====================================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_canton_console_scripting
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaults(ProtocolVersion.forSynchronizer))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. hidden:: participant2.synchronizers.connect_local(sequencer1, "da")
    .. hidden:: val myParticipant = participant1
    .. hidden:: val syncAlias = "mysynchronizer"
    .. success:: val id = participant1.id

.. snippet:: cn_global_synchronizer_canton_console_scripting
    .. success:: println(s"Participant ID: $id")

.. snippet:: cn_global_synchronizer_canton_console_scripting
    .. success:: participants.all.foreach { p => println(s"${p.name}: ${p.health.status}") }
    .. success:: participant1.health.status match { case status if status.isActive.contains(true) => println("Healthy"); case _ => println("Check this node") }

.. snippet:: cn_global_synchronizer_canton_console_scripting
    .. success:: participant1.health.ping(participant1, timeout = 30.seconds)

.. snippet:: cn_global_synchronizer_canton_console_scripting
    .. success:: nodes.local.foreach { node => val status = node.health.status; if (!status.isActive.getOrElse(false)) { println(s"WARNING: ${node.name} is not active") } }

.. snippet:: cn_global_synchronizer_canton_console_scripting
    .. success:: val roundTrip = participant1.health.ping(participant2, timeout = 30.seconds)
    .. success:: println(s"Round trip: ${roundTrip}")

.. snippet:: cn_global_synchronizer_canton_console_scripting
    .. success:: participant1.dars.upload("dars/CantonExamples.dar")

.. snippet:: cn_global_synchronizer_canton_console_scripting
    .. success:: participants.all.foreach { p => println(s"DARs on ${p.name}: ${p.dars.list().size}") }

.. snippet:: cn_global_synchronizer_canton_console_scripting
    .. success:: participant1.synchronizers.list_connected().foreach { sync => println(s"Connected to: ${sync.synchronizerAlias}") }

.. snippet:: cn_global_synchronizer_canton_console_scripting
    .. success:: participant1.parties.list().foreach { party => println(s"Party: ${party.party}") }
