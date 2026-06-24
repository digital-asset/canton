..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-faq:

Canton Network Documentation Snippets: Global Synchronizer FAQ
==============================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_faq
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaults(ProtocolVersion.forSynchronizer))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. hidden:: val alice = participant1.parties.enable("Alice")
    .. hidden:: val aliceParty = participant1.parties.hosted("Alice").head.party
    .. hidden:: participant1.pruning.set_schedule(cron= "0 */10 * * * ?", maxDuration = PositiveDurationSeconds.ofSeconds(3600), retention= PositiveDurationSeconds.ofSeconds(180*86400))
    .. success:: participant1.pruning.get_schedule()

.. snippet:: cn_global_synchronizer_faq
    .. success:: health.status
    .. success:: participant1.synchronizers.list_connected()

.. snippet:: cn_global_synchronizer_faq
    .. success:: participant1.parties.list()
    .. success:: participant1.packages.list()
    .. hidden:: participant1.dars.upload("dars/CantonExamples.dar")
    .. hidden:: val pkgIou = participant1.packages.find_by_module("Iou").head
    .. hidden:: val createIouCmd = ledger_api_utils.create(pkgIou.packageId,"Iou","Iou",Map("payer" -> aliceParty,"owner" -> aliceParty,"amount" -> Map("value" -> 100.0, "currency" -> "EUR"),"viewers" -> List()))
    .. hidden::  participant1.ledger_api.commands.submit(Seq(aliceParty), Seq(createIouCmd))
    .. success:: participant1.ledger_api.state.acs.of_party(aliceParty)

.. snippet:: cn_global_synchronizer_faq
    .. success:: participant1.health.dump()

.. snippet:: cn_global_synchronizer_faq
    .. success:: participant1.health.dump()

.. snippet:: cn_global_synchronizer_faq
    .. success:: health.status
