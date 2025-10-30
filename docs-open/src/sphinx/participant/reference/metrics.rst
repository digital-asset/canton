..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _reference-metrics:

Metrics
-------

The following sections contain the common metrics exposed for Daml services supporting a Prometheus metrics reporter.

For the metric types referenced below, see the `relevant Prometheus documentation <https://prometheus.io/docs/tutorials/understanding_metric_types/>`_.

Participant Metrics
~~~~~~~~~~~~~~~~~~~

..
    Dynamically generated content:
.. generatedinclude:: monitoring.rst.participant_metrics.inc

Sequencer Metrics
~~~~~~~~~~~~~~~~~

..
    Dynamically generated content:
.. generatedinclude:: monitoring.rst.sequencer_metrics.inc

Mediator Metrics
~~~~~~~~~~~~~~~~

..
    Dynamically generated content:
.. generatedinclude:: monitoring.rst.mediator_metrics.inc


.. _reference-health-metrics:

Health Metrics
~~~~~~~~~~~~~~

The following metrics are exposed for all components.

daml_health_status
^^^^^^^^^^^^^^^^^^

- **Description**: The status of the component
- **Values**:

  - **0**: Not healthy
  - **1**: Healthy

- **Labels**:

  - **component**: the name of the component being monitored

- **Type**: Gauge

.. _reference-grpc-metrics:

gRPC Metrics
~~~~~~~~~~~~

The following metrics are exposed for all gRPC endpoints. These metrics have the following common labels attached:

- **grpc_service_name**:
    fully qualified name of the gRPC service (e.g. ``com.daml.ledger.api.v1.ActiveContractsService``)

- **grpc_method_name**:
    name of the gRPC method (e.g. ``GetActiveContracts``)

- **grpc_client_type**:
    type of client connection (``unary`` or ``streaming``)

- **grpc_server_type**:
    type of server connection (``unary`` or ``streaming``)

- **service**:
    Canton service's name (e.g. ``participant``, ``sequencer``, etc.)

daml_grpc_server_duration_seconds
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Distribution of the durations of serving gRPC requests
- **Type**: Histogram

daml_grpc_server_messages_sent_total
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Total number of gRPC messages sent (on either type of connection)
- **Type**: Counter

daml_grpc_server_messages_received_total
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Total number of gRPC messages received (on either type of connection)
- **Type**: Counter

daml_grpc_server_started_total
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Total number of started gRPC requests (on either type of connection)
- **Type**: Counter

daml_grpc_server_handled_total
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Total number of handled gRPC requests
- **Labels**:

  - **grpc_code**: returned `gRPC status code <https://grpc.github.io/grpc/core/md_doc_statuscodes.html>`_ for the call (``OK``, ``CANCELLED``, ``INVALID_ARGUMENT``, etc.)

- **Type**: Counter

daml_grpc_server_messages_sent_bytes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Distribution of payload sizes in gRPC messages sent (both unary and streaming)
- **Type**: Histogram

daml_grpc_server_messages_received_bytes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Distribution of payload sizes in gRPC messages received (both unary and streaming)
- **Type**: Histogram

HTTP Metrics
~~~~~~~~~~~~
The following metrics are exposed for all HTTP endpoints. These metrics have the following common labels attached:

- **http_verb**:
    HTTP verb used for a given call (e.g. ``GET`` or ``PUT``)

- **host**:
    fully qualified hostname of the HTTP endpoint (e.g. ``example.com``)

- **path**:
    path of the HTTP endpoint (e.g. ``/v2/parties``)

- **service**:
    Daml service's name (``json_api`` for the JSON Ledger API Service)

daml_http_requests_duration_seconds
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Distribution of the durations of serving HTTP requests
- **Type**: Histogram

daml_http_requests_total
^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Total number of HTTP requests completed
- **Labels**:

  - **http_status**: returned `HTTP status code <https://en.wikipedia.org/wiki/List_of_HTTP_status_codes>`_ for the call

- **Type**: Counter

daml_http_websocket_messages_received_total
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Total number of WebSocket messages received
- **Type**: Counter

daml_http_websocket_messages_sent_total
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Total number of WebSocket messages sent
- **Type**: Counter

daml_http_requests_payload_bytes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Distribution of payload sizes in HTTP requests received
- **Type**: Histogram

daml_http_responses_payload_bytes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Distribution of payload sizes in HTTP responses sent
- **Type**: Histogram

daml_http_websocket_messages_received_bytes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Distribution of payload sizes in WebSocket messages received
- **Type**: Histogram

daml_http_websocket_messages_sent_bytes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Distribution of payload sizes in WebSocket messages sent
- **Type**: Histogram

.. _reference-pruning-metrics:

Pruning Metrics
~~~~~~~~~~~~~~~

The following metrics are exposed for all pruning processes. These metrics have the following labels:

- **phase**:
    The name of the pruning phase being monitored

daml_services_pruning_prune_started_total
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Total number of started pruning processes
- **Type**: Counter

daml_services_pruning_prune_completed_total
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: Total number of completed pruning processes
- **Type**: Counter

.. _reference-jvm-metrics:

JVM Metrics
~~~~~~~~~~~
The following metrics are exposed for the JVM, if enabled.

runtime_jvm_gc_time
^^^^^^^^^^^^^^^^^^^
- **Description**: Time spent in a given JVM garbage collector in milliseconds
- **Labels**:

  - **gc**: Garbage collector regions (eg: ``G1 Old Generation``, ``G1 New Generation``)

- **Type**: Counter

runtime_jvm_gc_count
^^^^^^^^^^^^^^^^^^^^
- **Description**: The number of collections that have occurred for a given JVM garbage collector
- **Labels**:

  - **gc**: Garbage collector regions (eg: ``G1 Old Generation``, ``G1 New Generation``)

- **Type**: Counter

runtime_jvm_memory_area
^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: JVM memory area statistics
- **Labels**:

  - **area**: Can be ``heap`` or ``non_heap``
  - **type**: Can be ``committed``, ``used`` or ``max``

runtime_jvm_memory_pool
^^^^^^^^^^^^^^^^^^^^^^^
- **Description**: JVM memory pool statistics
- **Labels**:

  - **pool**: Defined pool name.
  - **type**: Can be ``committed``, ``used`` or ``max``

