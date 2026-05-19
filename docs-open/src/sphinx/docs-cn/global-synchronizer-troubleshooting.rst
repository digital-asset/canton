..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-troubleshooting:

Canton Network Documentation Snippets: Global Synchronizer Troubleshooting
==========================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_troubleshooting
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. hidden:: val yourParty = participant1.parties.enable("YourParty")
    .. success:: participant1.parties.enable("PartyName", synchronize = None)

.. snippet:: cn_global_synchronizer_troubleshooting
    .. success:: participant1.dars.upload("dars/CantonExamples.dar")
    .. success:: participant1.topology.vetted_packages.list()

.. snippet:: cn_global_synchronizer_troubleshooting
    .. success:: participant1.parties.list().filter(_.party.toProtoPrimitive.contains("YourParty"))
    .. hidden:: val pkgIou = participant1.packages.find_by_module("Iou").head
    .. hidden:: val createIouCmd = ledger_api_utils.create(pkgIou.packageId,"Iou","Iou",Map("payer" -> yourParty,"owner" -> yourParty,"amount" -> Map("value" -> 100.0, "currency" -> "EUR"),"viewers" -> List()))
    .. hidden::  participant1.ledger_api.commands.submit(Seq(yourParty), Seq(createIouCmd))
    .. success:: participant1.ledger_api.state.acs.of_party(yourParty)

.. snippet:: cn_global_synchronizer_troubleshooting
    .. hidden:: participant1.pruning.set_schedule(cron= "0 */10 * * * ?", maxDuration = PositiveDurationSeconds.ofSeconds(3600), retention= PositiveDurationSeconds.ofSeconds(180*86400))
    .. success:: participant1.pruning.get_schedule()

.. snippet:: cn_global_synchronizer_troubleshooting
    .. success:: health.status
    .. success:: participant1.synchronizers.list_connected()
    .. success:: participant1.parties.list()
    .. success:: participant1.ledger_api.state.acs.of_all().size
