..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-sdks-tools-language-bindings-scala:

Canton Network Documentation Snippets: SDKs & Tools — Scala Bindings
=====================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_sdks_tools_language_bindings_scala
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.forSynchronizer))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. success:: participant1.parties.list()
    .. success:: participant1.dars.upload("dars/CantonExamples.dar")
    .. success:: participant1.health.status

.. snippet:: cn_sdks_tools_language_bindings_scala
    .. success:: participant1.synchronizers.list_connected()
    .. success:: participant1.topology.party_to_participant_mappings.list(participant1.synchronizers.list_connected().head.synchronizerId)
    .. success:: sequencer1.health.status
    .. success:: mediator1.health.status

.. snippet:: cn_sdks_tools_language_bindings_scala
    .. success:: val parties = participant1.parties.list()
    .. success:: val mappings = participant1.topology.party_to_participant_mappings.list(participant1.synchronizers.list_connected().head.synchronizerId)
    .. success:: println(s"Found ${parties.size} parties and ${mappings.size} topology mapping rows")

.. snippet:: cn_sdks_tools_language_bindings_scala
    .. hidden:: val scalaBindingsDemoScript = { val p = java.nio.file.Files.createTempFile("scala-bindings-demo", ".sc"); java.nio.file.Files.writeString(p, "println(\"backup-topology\")\n"); p.toAbsolutePath.toString }
    .. success:: interp.load.module(os.Path(scalaBindingsDemoScript))
