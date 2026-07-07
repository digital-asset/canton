..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-sdks-tools-cli-tools-canton-console:

Canton Network Documentation Snippets: SDKs & Tools — Canton Console
=====================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_sdks_tools_cli_tools_canton_console
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaults(ProtocolVersion.forSynchronizer))
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. success:: participant1.parties.list()
    .. success:: participant1.dars.upload("dars/CantonExamples.dar")
    .. success:: participant1.health.status
    .. success:: participant1.synchronizers.list_connected()
    .. success:: participant1.parties.enable("Alice")

.. snippet:: cn_sdks_tools_cli_tools_canton_console
    .. success:: participant1.parties.list()
    .. success:: participant1.dars.upload("dars/CantonExamples.dar")
    .. success:: participant1.synchronizers.list_connected()

.. snippet:: cn_sdks_tools_cli_tools_canton_console
    .. success:: sequencer1.health.status

.. snippet:: cn_sdks_tools_cli_tools_canton_console
    .. success:: mediator1.health.status

.. snippet:: cn_sdks_tools_cli_tools_canton_console
    .. hidden:: val cantonConsoleDemoScript = { val p = java.nio.file.Files.createTempFile("canton-console-demo", ".sc"); java.nio.file.Files.writeString(p, "println(42)\n"); p.toAbsolutePath.toString }
    .. success:: interp.load.module(os.Path(cantonConsoleDemoScript))
