..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _docs-cn-global-synchronizer-reference-canton-console-reference:

Canton Network Documentation Snippets: Reference Canton Console Reference
=========================================================================

.. NOTE: This file is a test harness for the Canton Network documentation site.
.. It is never rendered as a documentation page. The snippet directives here
.. are executed by SphinxDocumentationGenerator integration tests.

.. snippet:: cn_global_synchronizer_reference_canton_console_reference
    .. success:: val status = participant1.health.status
    .. success:: println(s"Participant status: $status")
    .. success:: val connected = participant1.synchronizers.list_connected()
    .. success:: println(s"Connected synchronizers: ${connected.size}")
    .. success:: connected.foreach { sync => println(s"  - ${sync.synchronizerAlias}") }
