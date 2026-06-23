..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-troubleshooting-guide-transaction-failures:

Canton Network Documentation Snippets: Troubleshooting Guide Transaction Failures
=================================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_troubleshooting_guide_transaction_failures
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaults(ProtocolVersion.forSynchronizer))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. success:: participant1.parties.list()

.. snippet:: cn_global_synchronizer_troubleshooting_guide_transaction_failures
    .. success:: participant1.dars.upload("dars/CantonExamples.dar")

.. snippet:: cn_global_synchronizer_troubleshooting_guide_transaction_failures
    .. success:: participant1.topology.vetted_packages.list()
    .. success:: participant1.packages.list().filter(_.packageId.toString.nonEmpty)

.. snippet:: cn_global_synchronizer_troubleshooting_guide_transaction_failures
    .. success:: participant1.traffic_control.traffic_state(participant1.synchronizers.id_of("da"))
