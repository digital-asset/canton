..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-debugging-tools:

Canton Network Documentation Snippets: Debugging tools
======================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_debugging_tools
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaults(ProtocolVersion.forSynchronizer))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. hidden:: val myParty = participant1.parties.enable("MyParty")
    .. hidden:: participant1.dars.upload("dars/CantonExamples.dar")
    .. hidden:: val pkgIou = participant1.packages.find_by_module("Iou").head
    .. hidden:: val createIouCmd = ledger_api_utils.create(pkgIou.packageId,"Iou","Iou",Map("payer" -> myParty,"owner" -> myParty,"amount" -> Map("value" -> 100.0, "currency" -> "EUR"),"viewers" -> List()))
    .. hidden::  participant1.ledger_api.commands.submit(Seq(myParty), Seq(createIouCmd))
    .. success:: participant1.ledger_api.state.acs.of_party(myParty)
    .. success:: participant1.ledger_api.state.acs.of_party(myParty).filter(_.templateId.toString.contains("Iou"))

.. snippet:: cn_debugging_tools
    .. success:: participant1.ledger_api.completions.list(myParty, atLeastNumCompletions = 1, beginOffsetExclusive = 0L)

.. snippet:: cn_debugging_tools
    .. success:: participant1.dars.upload("dars/CantonExamples.dar")
