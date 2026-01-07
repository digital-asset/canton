..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _mediator-health:

Check and monitor Mediator Node health
======================================

This page describes how to inspect and understand the current status of a Mediator Node,
and how to continuously monitor it using health checks.

See also the generic guide on :externalref:`How to check the status of a Node and monitor its health <observe-health>`.



Interactively check Node status
-------------------------------

Canton console can be used to interactively inspect the state and get information about a running Mediator Node.
Execute the following command against a ``mediator`` reference of interest.

.. code-block:: scala

    mediator.health.status

For a Mediator Node that has never been connected to a Synchronizer, the output looks like this:

.. snippet:: mediator_health
    .. success:: mediator1.health.status

.. snippet:: mediator_health
    .. hidden:: bootstrap.synchronizer(
          synchronizerName = "da",
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest),
        )

.. snippet:: mediator_health
    .. hidden:: mediator1.health.wait_for_initialized()

For a healthy state expect the Mediator Node to report a status similar to:

.. snippet:: mediator_health
    .. success:: mediator1.health.status

The components status includes ``db-storage`` for the database storage backend, and ``sequencer-client``.
The latter not being ``Ok()`` indicates problems with the Synchronizer connectivity.

The status also shows `Active: true` or `Active: false` for the Mediator Node, which in High Availability (HA)
configuration indicates whether this Mediator Node is the active HA replica or not.



Health check endpoints
----------------------

To monitor the health of a Mediator Node with external tools, use the Canton Node health check endpoints.

Enabling the endpoint is described in the generic guide on :externalref:`How to check the status of a Node and monitor its health <observe-health>`.

A Mediator Node exposes a pair of health check endpoints: ``readiness`` (accepts traffic)
and ``liveness`` (does not require a restart), to be used respectively for load balancing and orchestration
with tools like Kubernetes.

The ``readiness`` endpoint corresponds to the health of the storage backend of the Mediator,
and the ``liveness`` endpoint corresponds to the health of ``sequencer-client`` component
in the status command output described above. This means that a fatal failure of the Sequencer connection
of the Mediator Node requires a restart of the Mediator Node.



Liveness watchdog
-----------------

A Mediator Node can be configured to automatically exit when it becomes unhealthy.
The following configuration enables an internal watchdog service that checks the Mediator Node health
every ``check-interval`` seconds and kills the process after ``kill-delay`` seconds after
the ``liveness`` reports the Node unhealthy.

.. literalinclude:: CANTON/community/app/src/test/resources/sequencer-mediator-health-documentation.conf
   :language: none
   :start-after: mediator_health-watchdog-example-begin
   :end-before: mediator_health-watchdog-example-end

Place the above under ``canton.mediators.mediator.parameters`` in the configuration file of the Mediator Node.
