..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-canton-console-essential-commands:

Canton Network Documentation Snippets: Global Synchronizer Canton Console Essential Commands
==============================================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. hidden:: val participant = participant1
    .. hidden:: val synchronizerAlias = participant1.synchronizers.list_connected().head.synchronizerAlias
    .. hidden:: val synchronizerId = participant1.synchronizers.list_connected().head.synchronizerId
    .. hidden:: val partyId = participant1.parties.enable("Alice")
    .. success:: participant1.health.status
    .. success:: participant1.health.is_running
    .. success:: participant1.synchronizers.list_connected()

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: sequencer1.health.status

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: participant1.parties.hosted()
    .. success:: participant1.parties.hosted("Alice")
    .. success:: participant1.parties.list()

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: participant1.parties.hosted("Alice").head

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: participant1.dars.list()
    .. success:: participant1.packages.list().filter(_.packageId.startsWith("com-example"))

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: participant1.dars.upload("dars/CantonExamples.dar")

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: participant1.topology.vetted_packages.list()
    .. hidden:: val packageId = participant1.topology.vetted_packages.list().head.item.packages.head.packageId.toString
    .. success:: participant1.topology.vetted_packages.list().filter(_.item.packages.exists(_.packageId.toString.contains(packageId)))

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: participant1.topology.party_to_participant_mappings.list(synchronizerId)

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: participant1.topology.synchronizer_parameters.list(synchronizerId)

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: participant1.topology.namespace_delegations.list(store = synchronizerId)

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: participant1.synchronizers.list_connected()

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: val syncAlias = participant1.synchronizers.list_connected().head.synchronizerAlias
    .. hidden:: participant1.synchronizers.disconnect(syncAlias)
    .. success:: participant1.synchronizers.reconnect(syncAlias)

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. hidden:: participant1.dars.upload("dars/CantonExamples.dar")
    .. hidden:: val pkgIou = participant1.packages.find_by_module("Iou").head
    .. hidden:: val createIouCmd = ledger_api_utils.create(pkgIou.packageId,"Iou","Iou",Map("payer" -> partyId,"owner" -> partyId,"amount" -> Map("value" -> 100.0, "currency" -> "EUR"),"viewers" -> List()))
    .. hidden::  participant1.ledger_api.commands.submit(Seq(partyId), Seq(createIouCmd))
    .. success:: participant1.testing.acs_search(synchronizerAlias, filterTemplate = "Iou:Iou").size
    .. success:: participant1.testing.acs_search(synchronizerAlias, filterTemplate = "Iou:Iou", filterStakeholder = Some(partyId))

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: val participantId = participant1.id

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: val partyId2 = participant1.parties.hosted("Alice").head.party

.. snippet:: cn_global_synchronizer_canton_console_essential_commands
    .. success:: val syncId2 = participant1.synchronizers.list_connected().head.synchronizerId
