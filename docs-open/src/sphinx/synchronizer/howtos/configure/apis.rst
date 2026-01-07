..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _synchronizer-api-configuration:

Configure Synchronizer APIs
===========================

A Synchronizer exposes two main APIs, the Admin API and the Public API, while the Participant Node
exposes the Ledger API and the Admin API. In this section, we explain what the APIs do and how
they can be configured.

For details on how to configure endpoints and their addresses, ports, keep alive, and so on, see
the :externalref:`general API documentation<old_api_configuration>`.

.. _public-api-configuration:

Configure Sequencer Public API
------------------------------

The Sequencer Public API provides the services to other nodes to connect, authenticate, and exchange messages with a Synchronizer.
To learn more about the Sequencer's role, visit the :brokenref:`Sequencer overview page<sequencer-overview>`.

Configure the Public API ``public-api`` under a Sequencer node configuration:

.. literalinclude:: CANTON/community/app/src/pack/examples/01-simple-topology/simple-topology.conf
   :language: scala
   :start-after: user-manual-entry-begin: SimpleSequencerNodeConfig
   :end-before: user-manual-entry-end: SimpleSequencerNodeConfig
   :dedent:

.. todo::
    #. Link the reference documentation. <https://github.com/DACH-NY/canton/issues/26126>

All Sequencer Public API configuration parameters have defaults. For example, the default address to listen on is ``127.0.0.1``.
To find out more about the parameters, check the reference documentation.

Configure Sequencer Admin API
-----------------------------

The Sequencer Admin API can be configured in the :externalref:`standard way<admin-api-configuration>` under the Sequencer node configuration
(at the same level as the Public API).

Configure Mediator Admin API
----------------------------

The Mediator Admin API can be configured in the :externalref:`standard way<admin-api-configuration>` under the Mediator node configuration:

.. literalinclude:: CANTON/community/app/src/pack/examples/01-simple-topology/simple-topology.conf
   :language: scala
   :start-after: user-manual-entry-begin: SimpleMediatorNodeConfig
   :end-before: user-manual-entry-end: SimpleMediatorNodeConfig
   :dedent:
