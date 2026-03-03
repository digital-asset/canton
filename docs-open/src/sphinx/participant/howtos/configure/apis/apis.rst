..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Separate security hardening and move it into the "secure" section (TLS, max message size, authzn)
    Separate optimizations such as caching to "optimize" section
    Move API specific configuration to their sub-sections, only cover general configuration here (such as ports, keep alive, netty).

.. _old_api_configuration:

API Configuration
=================

Participant nodes expose the Admin API, the gRPC Ledger API, and optionally the JSON Ledger API.

This page explains how to configure general options that apply to both the Admin API and the gRPC Ledger API.
For the configuration options that apply only to specific APIs, please refer to the :ref:`Administration API<admin-api-configuration>`,
the :ref:`gRPC Ledger API<ledger-api-configuration>`, and the :ref:`JSON Ledger API<json-api-configuration>` pages.

Ports
-----

Ports for the Admin API, gRPC Ledger API and JSON Ledger API have to be provided explicitly:

.. literalinclude:: CANTON/community/app/src/pack/examples/01-simple-topology/simple-topology.conf
   :start-after: user-manual-entry-begin: port configuration
   :end-before: user-manual-entry-end: port configuration
   :dedent:

Note that if JSON Ledger API is disabled, then its port does not have to be provided:

.. literalinclude:: CANTON/community/app/src/pack/examples/05-composability/composability.conf
   :start-after: user-manual-entry-begin: disable json ledger api
   :end-before: user-manual-entry-end: disable json ledger api
   :dedent:

.. _keepalive-configuration:

Keep Alive
----------

Canton enables keep-alive by default on all gRPC connections in order to prevent load-balancers or firewalls from terminating
long-running RPC calls in the event of some silence on the connection.

To tweak the keep-alive configuration of a connection, adjust the following parameters:

* ``time``
* ``timeout``
* ``permit-keep-alive-time``
* ``permit-keep-alive-without-calls``
* ``keep-alive-withouth-calls``
* ``idle-timeout``

Canton sets different default values for these parameters depending on the API:

+---------------------------------+------------+------------+------------+
| Config                          | Admin API  | Ledger API | Client     |
+=================================+============+============+============+
| time                            | 40s        | 10min      | 40s        |
+---------------------------------+------------+------------+------------+
| timeout                         | 20s        | 20s        | 15s        |
+---------------------------------+------------+------------+------------+
| keep-alive-without-calls        | N/A        | N/A        | false      |
+---------------------------------+------------+------------+------------+
| idle-timeout                    | N/A        | N/A        | 30min      |
+---------------------------------+------------+------------+------------+
| permit-keep-alive-time          | 20s        | 10s        | N/A        |
+---------------------------------+------------+------------+------------+
| permit-keep-alive-without-calls | false      | false      | N/A        |
+---------------------------------+------------+------------+------------+

Client values can be adjusted in the ``keep-alive-client`` section of the configuration,
while server values can be adjusted in the ``keep-alive-server`` of the configuration.

.. important::
    ``keep-alive-without-calls`` can have a negative performance impact. Be cautious when turning it on, and in general prefer using ``idle-timeout`` when possible.

``permit-keep-alive-time`` specifies the most aggressive keep-alive time that a client is permitted to use.
If a client uses a keep-alive ``time`` that is more aggressive than the server's ``permit-keep-alive-time``,
the connection is terminated with a ``GOAWAY`` error with “too_many_pings” as the debug data.
Setting ``permit-keep-alive-without-calls`` to ``true`` allows clients to send ping messages outside of any ongoing gRPC call. Such a ping otherwise results in a ``GOAWAY`` error.
When ``keep-alive-without-calls`` is enabled, ``permit-keep-alive-without-calls`` must be enabled on the server side, and ``permit-keep-alive-time`` adjusted to allow for a potentially higher frequency of keep alives coming from the client.

.. tip::
    The value for ``idle-timeout`` should be set lower than timeouts in the network stack between client and server.
    In particular, check the idle timeout configuration of Load Balancers. Defaults for `AWS ALB <https://docs.aws.amazon.com/elasticloadbalancing/latest/application/application-load-balancers.html>`__, `AWS NLB <https://docs.aws.amazon.com/elasticloadbalancing/latest/network/update-idle-timeout.html>`__, `GCP <https://docs.cloud.google.com/load-balancing/docs/https/request-distribution#http-keepalive-timeout>`__.

The `gRPC documentation <https://grpc.io/docs/guides/keepalive/>`__ further describes these parameters and their effect.

The following is an example that demonstrates how to configure the keep-alive for the various APIs:

.. literalinclude:: CANTON/community/app/src/pack/config/keep-alive/keep-alive.conf
   :start-after: user-manual-entry-begin: keep-alive configuration
   :end-before: user-manual-entry-end: keep-alive configuration



Native libraries usage by Netty
-------------------------------

Canton ships with native libraries (for some processor architectures: x86_64, ARM64, S390_64) so that the Netty network access library
can take advantage of the ``epoll`` `system call <https://en.wikipedia.org/wiki/Epoll>`__ on Linux. This generally leads to
improved performance and less pressure on the JVM garbage collector.

The system automatically picks the native library if available for the current operating system and architecture, or falls back to
the standard NIO library if the native library is not available.

To switch off using the native library, set the following when running Canton:

.. code-block:: text

    -Dio.grpc.netty.shaded.io.netty.transport.noNative=true

Even when this is expected, falling back to NIO might lead to a warning being emitted at ``DEBUG`` level on your log.
