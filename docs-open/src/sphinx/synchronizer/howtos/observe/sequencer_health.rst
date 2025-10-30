..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _sequencer-health:

Check and monitor Sequencer Node health
=======================================

This page describes how to inspect and understand the current status of a Sequencer Node,
and how to continuously monitor it using health checks.

See also the generic guide on :externalref:`How to check the status of a Node and monitor its health <observe-health>`.

Interactively check Node status
-------------------------------

Canton console can be used to interactively inspect the state and get information about a running Sequencer Node.
Execute the following command against a ``sequencer`` reference of interest.

.. code-block:: scala

    sequencer.health.status

For a Sequencer Node that has never been connected to a Synchronizer, the output looks like this:

.. snippet:: sequencer_health
    .. success:: sequencer1.health.status

.. snippet:: sequencer_health
    .. hidden:: bootstrap.synchronizer(
          synchronizerName = "da",
          sequencers = Seq(sequencer1, sequencer2, sequencer3),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1, sequencer2, sequencer3),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
        )

.. snippet:: sequencer_health
    .. hidden:: mediator1.health.wait_for_initialized()

.. snippet:: sequencer_health
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "synchronizerAlias")
        participant2.synchronizers.connect_local(sequencer1, "synchronizerAlias")
        participant3.synchronizers.connect_local(sequencer1, "synchronizerAlias")

For a healthy state expect the Sequencer Node to report a status similar to:

.. snippet:: sequencer_health
    .. success:: sequencer1.health.status

The components status includes ``sequencer`` for the Sequencer Node itself, and ``db-storage``.
The latter not being ``Ok()`` indicates problems with the database storage backend or connectivity to it.

The status also shows the Synchronizer members connected to the Sequencer Node. These members have an active
subscription to the Sequencer Node. They can send new submissions to, and read ordered transactions from the Synchronizer.



BFT Orderer peer network status
-------------------------------

For a BFT (Byzantine Fault Tolerance) Sequencer Node, you can inspect the connection status to the other peers of its BFT peer network.

.. snippet:: sequencer_health
    .. success:: sequencer1.bft.get_peer_network_status()



Health check endpoints
----------------------

To monitor the health of a Sequencer Node with external tools, use the Canton Node health check endpoints.

Enabling the endpoint is described in the generic guide on :externalref:`How to check the status of a Node and monitor its health <observe-health>`.

A Sequencer Node exposes a pair of health check endpoints: ``readiness`` (accepts traffic)
and ``liveness`` (does not require a restart), to be used respectively for load balancing and orchestration
with tools like Kubernetes.

The ``readiness`` endpoint corresponds to the health of ``sequencer`` (including the BFT Orderer),
and the ``liveness`` endpoint corresponds to the health of ``db-storage`` component
in the status command output described above. This means that a fatal failure of the database connection
causes a restart of the Sequencer Node.



Liveness watchdog
-----------------

A Sequencer Node can be configured to automatically exit when it becomes unhealthy.
The following configuration enables an internal watchdog service that checks the Sequencer Node health
every ``check-interval`` seconds and kills the process after ``kill-delay`` seconds after
the ``liveness`` reports the Node unhealthy.

.. literalinclude:: CANTON/enterprise/app/src/test/resources/sequencer-mediator-health-documentation.conf
   :language: none
   :start-after: sequencer_health-watchdog-example-begin
   :end-before: sequencer_health-watchdog-example-end

Place the above under ``canton.sequencers.sequencer.parameters`` in the configuration file of the Sequencer Node.

