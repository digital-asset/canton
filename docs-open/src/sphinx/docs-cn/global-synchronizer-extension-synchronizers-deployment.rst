..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-extension-synchronizers-deployment:

Canton Network Documentation Snippets: Extension Synchronizers Deployment
==========================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_extension_synchronizers_deployment
    .. success:: bootstrap.synchronizer(synchronizerName = "my-private-sync", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaults(ProtocolVersion.forSynchronizer))

.. snippet:: cn_global_synchronizer_extension_synchronizers_deployment
    .. success:: participant1.synchronizers.connect_local(sequencer1, "my-private-sync")
