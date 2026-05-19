..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-extension-synchronizers-hybrid-synchronizer-pattern:

Canton Network Documentation Snippets: Extension Synchronizers Hybrid Synchronizer Pattern
===========================================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_extension_synchronizers_hybrid_synchronizer_pattern
    .. success:: bootstrap.synchronizer(synchronizerName = "private-sync", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer))
    .. success:: participant1.synchronizers.connect_local(sequencer1, "private-sync")
