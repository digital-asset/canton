..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-canton-console-debugging-workflows:

Canton Network Documentation Snippets: Global Synchronizer Canton Console Debugging Workflows
===============================================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_canton_console_debugging_workflows
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaults(ProtocolVersion.forSynchronizer))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. hidden:: val synchronizerAlias = participant1.synchronizers.list_connected().head.synchronizerAlias
    .. hidden:: val synchronizerId = participant1.synchronizers.list_connected().head.synchronizerId
    .. hidden:: val targetPackageId = "dummy-package-id"
    .. hidden:: val targetPartyId = participant1.parties.enable("Alice")
    .. hidden:: val participant = participant1
    .. success:: participant1.health.status

.. snippet:: cn_global_synchronizer_canton_console_debugging_workflows
    .. success:: participant1.synchronizers.list_connected()

.. snippet:: cn_global_synchronizer_canton_console_debugging_workflows
    .. success:: participant1.synchronizers.reconnect(synchronizerAlias)

.. snippet:: cn_global_synchronizer_canton_console_debugging_workflows
    .. success:: participant1.topology.vetted_packages.list()

.. snippet:: cn_global_synchronizer_canton_console_debugging_workflows
    .. success:: participant1.topology.vetted_packages.list().filter(_.item.packages.exists(_.packageId.toString.contains(targetPackageId)))
    .. success:: participant1.dars.upload("dars/CantonExamples.dar")

.. snippet:: cn_global_synchronizer_canton_console_debugging_workflows
    .. failure:: participant1.synchronizers.connect("unreachable_synchronizer", "http://non-existent-sequencer.local:12345")

.. snippet:: cn_global_synchronizer_canton_console_debugging_workflows
    .. success:: sequencer1.health.status

.. snippet:: cn_global_synchronizer_canton_console_debugging_workflows
    .. success:: participant1.parties.list().filter(_.party.toProtoPrimitive.startsWith("Bob"))

.. snippet:: cn_global_synchronizer_canton_console_debugging_workflows
    .. hidden:: participant2.synchronizers.connect_local(sequencer1, "da")
    .. hidden:: participant1.topology.party_to_participant_mappings.propose(
        targetPartyId,
        Seq(
          (participant1.id, ParticipantPermission.Confirmation),
          (participant2.id, ParticipantPermission.Confirmation),
        ),
        store = synchronizerId,
      )
    .. success:: participant1.topology.party_to_participant_mappings.list_hosting_proposals(synchronizerId, participant1.id)

.. snippet:: cn_global_synchronizer_canton_console_debugging_workflows
    .. hidden:: participant1.dars.upload("dars/CantonExamples.dar")
    .. hidden:: val pkgIou = participant1.packages.find_by_module("Iou").head
    .. hidden:: val createIouCmd = ledger_api_utils.create(pkgIou.packageId,"Iou","Iou",Map("payer" -> targetPartyId,"owner" -> targetPartyId,"amount" -> Map("value" -> 100.0, "currency" -> "EUR"),"viewers" -> List()))
    .. hidden::  participant1.ledger_api.commands.submit(Seq(targetPartyId), Seq(createIouCmd))
    .. success:: val contracts = participant1.testing.acs_search(synchronizerAlias, filterTemplate = "Iou:Iou")
    .. success:: s"Found ${contracts.size} active contracts"
    .. success:: contracts.headOption.map { c => (c.contractId, c.toLf.arg) }
