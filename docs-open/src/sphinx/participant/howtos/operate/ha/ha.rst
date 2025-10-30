..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Document howto enable replication (on by default) on enterprise nodes with supported storage.
    Document health check configuration and fail-over times.
    Document admin commands to work with multiple replicas (find active replica), document commands to inspect activeness.
    For participant: load balancer configuration in front of gRPC Ledger API to route to active instance.
    Link to explanation on HA architecture.

.. enterprise-only::

.. _ha_user_manual:

High Availability Usage
=======================

This section looks at some of the components already mentioned and supplies useful Canton commands.


Participant
-----------

High availability of a participant node is achieved by running multiple
participant node replicas that have access to a shared database.

Participant node replicas are configured in the Canton configuration file as
individual participants with two required changes for each participant node
replica:

- Using the same storage configuration to ensure access to the shared database.
  Only PostgreSQL and Oracle-based storage is supported for HA. For Oracle it is crucial that the participant replicas
  use the same username to access the shared database.
- Set ``replication.enabled = true`` for each participant node replica.

.. note::

    Starting from Canton 2.4.0, participant replication is enabled by default when using supported storage.

Manual trigger of a fail-over
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fail-over from the active to a passive replica is done automatically when the active replica has a failure, but one can also initiate a graceful fail-over
with the following command:

.. literalinclude:: CANTON/enterprise/app/src/test/scala/com/digitalasset/canton/integration/tests/ReplicatedParticipantTest.scala
   :language: scala
   :start-after: user-manual-entry-begin: SetPassive
   :end-before: user-manual-entry-end: SetPassive
   :dedent:

The command succeeds if there is at least another passive replica that takes
over from the current active replica, otherwise the active replica remains
active.

.. _load-balancer-configuration:

Load balancer configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many replicated participants can be placed behind an appropriately sophisticated load balancer that will by health checks
determine which participant instance is active and direct ledger and admin api requests to that instance appropriately.
This makes participant replication and failover transparent from the perspective of the ledger-api application or canton console
administering the logical participant, as they will simply be pointed at the load balancer.

Participants should be configured to expose an "IsActive" health status on our health HTTP server using the following
monitoring configuration:

.. literalinclude:: CANTON/enterprise/app/src/test/resources/health-check-is-active.conf
   :start-after: user-manual-entry-begin: IsActiveHealthCheck
   :end-before: user-manual-entry-end: IsActiveHealthCheck

Once running, this server reports a HTTP 200 status code on a http/1 GET request to `/health` if the Participant
is currently the active replica.
Otherwise, an error will be returned.

To use a load balancer it must support http/1 health checks for routing requests on a separate http/2 (GRPC) server.
This is possible with `HAProxy <http://www.haproxy.org/>`__ using the following example configuration::

  global
      log stdout format raw local0

  defaults
      log global
      mode http
      option httplog
      # enabled so long running connections are logged immediately upon connect
      option logasap

  # expose the admin-api and ledger-api as separate servers
  frontend admin-api
      bind :15001 proto h2
      default_backend admin-api

  backend admin-api
      # enable HTTP health checks
      option httpchk
      # required to create a separate connection to query the load balancer.
      # this is particularly important as the health HTTP server does not support h2
      # which would otherwise be the default.
      http-check connect
      # set the health check uri
      http-check send meth GET uri /health

      # list all participant backends
      server participant1 participant1.lan:15001 proto h2 check port 8080
      server participant2 participant2.lan:15001 proto h2 check port 8080
      server participant3 participant3.lan:15001 proto h2 check port 8080

  # repeat a similar configuration to the above for the ledger-api
  frontend ledger-api
      bind :15000 proto h2
      default_backend ledger-api

  backend ledger-api
      option httpchk
      http-check connect
      http-check send meth GET uri /health

      server participant1 participant1.lan:15000 proto h2 check port 8080
      server participant2 participant2.lan:15000 proto h2 check port 8080
      server participant3 participant3.lan:15000 proto h2 check port 8080
