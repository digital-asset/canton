..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _observe-health:

Participant Node Health
=======================

The participant exposes health status information in several ways, which may be inspected manually when troubleshooting or integrated into larger monitoring and orchestration systems.


Using gRPC Health Service for Load Balancing and Orchestration
--------------------------------------------------------------

The Participant Node provides a ``grpc.health.v1.Health`` service, implementing the `gRPC Health Checking Protocol <https://github.com/grpc/grpc/blob/master/doc/health-checking.md>`_ protocol.

Kubernetes containers can be `configured <https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-grpc-liveness-probe>`_
to use this for readiness or liveness `probes <https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/#readiness-probe>`_, e.g.

::

    readinessProbe:
      grpc:
        port: <port>

By default the port is the one used for the `Ledger API`.

Likewise, `gRPC clients <https://grpc.io/docs/guides/health-checking/#enabling-client-health-checking>`_
and `NGinx <https://docs.nginx.com/nginx/admin-guide/load-balancer/grpc-health-check/>`_
can be configured to watch the health service for traffic management and load balancing.

You can manually check the health of a Participant with a command line tool such as
`grpcurl <https://github.com/fullstorydev/grpcurl>`_
e.g. (using the Participant's actual address):

::

    $ grpcurl -plaintext <host>:<port> grpc.health.v1.Health/Check
    {
      "status": "SERVING"
    }


Calling `Check <https://github.com/grpc/grpc-proto/blob/6565a1ba38af695ace7c3ce6e6ff837ee87d4c10/grpc/health/v1/health.proto#L55>`_
will respond with ``SERVING`` if it is currently ready and available to serve requests.

Calling `Watch <https://github.com/grpc/grpc-proto/blob/6565a1ba38af695ace7c3ce6e6ff837ee87d4c10/grpc/health/v1/health.proto#L72>`_
will perform a streaming health check.
The server will immediately send the current health of the Participant, and then send a new message whenever the health changes.

When :ref:`multiple Participant replicas <ha_user_manual>` are configured, passive nodes return ``NOT_SERVING``.

In practice, the health of the Participant is composed of the health of the components it depends on.
You can query these individually by name, by making a request with the `service <https://github.com/grpc/grpc-proto/blob/6565a1ba38af695ace7c3ce6e6ff837ee87d4c10/grpc/health/v1/health.proto#L29>`_ field set to the name of the component.
An empty or unset ``service`` field returns the aggregate health of all components.
An unknown name will result in a gRPC ``NOT_FOUND`` error.


Checking health via HTTP
------------------------

Health checking can also be done via HTTP, which is useful for frameworks that don't support gRPC Health Checking Protocol.
Setting ``monitoring.http-health-server.port=<port>`` in the configuration for your node will expose health information at the URL ``http://<host>:<port>/health``.

Here the important information is reported via the HTTP Reponse status code.

* A status of ``200`` is equivalent to ``SERVING`` from the gRPC Health Service.
* A status of ``503`` is equivalent to ``NOT_SERVING``.
* A status of ``500`` means the check failed for any other reason.

Kubernetes can use also use these for readiness probes:

::

    readinessProbe:
      httpGet:
        port: <port>
        path: /health


Inspection of General Health Status
-----------------------------------

General information about the Participant Node, including about unhealthy synchronizers and dependencies,
and whether the node is currently Active, can be displayed in the canton console by invoking the ``health.status`` command on the node.


.. snippet:: howtos_health
    .. hidden:: bootstrap.synchronizer(synchronizerName = "da", sequencers = Seq(sequencer1), mediators = Seq(mediator1), synchronizerOwners = Seq(sequencer1, mediator1), synchronizerThreshold = PositiveInt.one, staticSynchronizerParameters = StaticSynchronizerParameters.defaultsWithoutKMS(ProtocolVersion.latest))
    .. success:: participant1.health.status


The Admin API of the Participant Node provides programmatic access to this data in a structured form, via
`ParticipantStatusService <https://github.com/DACH-NY/canton/blob/release-line-3.3/community/admin-api/src/main/protobuf/com/digitalasset/canton/admin/participant/v30/participant_status_service.proto#L10>`_'s
``ParticipantStatus`` call.

.. TODO(i26029) add link to proper reference data


The canton console can also provide information about *all* connected nodes, including those remotely connected, by invoking the command at the top level.

.. snippet:: howtos_health
    .. hidden:: participant1.synchronizers.connect_local(sequencer1, "da")
    .. assert:: participant1.synchronizers.list_connected.nonEmpty
    .. hidden:: participant2.synchronizers.connect_local(sequencer1, "da")
    .. assert:: participant2.synchronizers.list_connected.nonEmpty
    .. success:: health.status

Generating a Node Health Dump for Troubleshooting
-------------------------------------------------

When interacting with support or attempting to troubleshoot an issue, it is often necessary to capture a snapshot of relevant execution state.
Canton implements a facility that gathers key system information and bundles it into a ZIP file.

This will contain:

* The configuration you are using, with all sensitive data stripped from it (no passwords).
* An extract of the log file. Sensitive data is not logged into log files.
* A current snapshot on Canton metrics.
* A stacktrace for each running thread.

These health dumps can be triggered from the canton console with ``health.dump()``, which returns the path to the resulting ZIP file.


.. snippet:: howtos_health
   .. success(output=0):: health.dump()

If the console is configured to access remote nodes, their state will be included too.
You can obtain the data of just a specific node by targeting it when running the command, e.g. ``remoteParticipant1.health.dump()``

When packaging large amounts of data, increase the default timeout of the dump command:

.. snippet:: howtos_health
   .. success(output=0):: health.dump(timeout = 2.minutes)


Health dumps can also be gathered via gRPC on the `Admin API` of the Participant Node via the `StatusService <https://github.com/DACH-NY/canton/blob/release-line-3.3/community/admin-api/src/main/protobuf/com/digitalasset/canton/admin/health/v30/status_service.proto#L10>`_'s ``HealthDump``. This call streams back the bytes of the produced ZIP file.


Monitoring for Slow or Stuck Tasks
----------------------------------

Some operations can report when they are slow, if you enable

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/log-slow-futures.conf

If a task is taking longer than expected, a log line will be emitted periodically until it completes, such as ``<task name> has not completed after <duration>``.
This feature is disabled by default to reduce the overhead.

Canton also provides a facility to periodically test whether we are able to schedule new tasks in a timely manner, enabled via the configuration

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/deadlock-detection.conf

If a problem is detected, a log line containing ``Task runner <name> is stuck or overloaded for <duration>`` will be emitted.
This may indicate that resources such as CPU are overloaded, that the Execution Context is too small, or that too many tasks are otherwise stuck.
If the issue resolves itself, a subsequent log message: ``Task runner <name> is just overloaded, but operating correctly. Task got executed in the meantime``
will be emitted.


Disabling Restart on Fatal Failures
-----------------------------------

Processes should be run under a process supervisor, such as ``systemd`` or Kubernetes, which can monitor them and restart them as needed.
By default, the Participant Node process will exit in the event of a fatal failure.

If you wish to disable this behaviour

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/exit-on-fatal-failures.conf

which will cause the Node to stay alive and report unhealthy in such cases.
