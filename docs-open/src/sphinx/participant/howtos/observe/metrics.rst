..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Cover metrics gathering.
    Move content from "monitoring.rst" here.
    Link to example dashboards (if already available then).

.. _observe-metrics:

Configure Metrics
=================

Metrics provide quantitative information about the internal state of a running system, which is key for `monitoring <https://sre.google/sre-book/monitoring-distributed-systems/>`__ of its health and performance and `observability <https://medium.com/honeycombio/observability-101-terminology-and-concepts-honeycomb-821f17fde452>`__ of its operating behavior.

Participant Nodes can report these metrics on an HTTP endpoint. This should then be periodically scraped by a separate monitoring system such as `Prometheus <https://prometheus.io>`__, which can store it as time-series data for querying, alerting, dashboards, etc.

Export Metrics for Scraping
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Export application metrics by enabling the Prometheus reporter with the configuration
``canton.monitoring.metrics.reporters = [{ type = prometheus }]``

Metrics will be available for scraping in `OpenMetrics <https://prometheus.io/docs/specs/om/open_metrics_spec/>`_ format at ``http://<host>:9464/``, by default.

You can also export JVM metrics with

``canton.monitoring.metrics.reporters.jvm-metrics.enabled = yes``

e.g.

.. literalinclude:: CANTON/community/app/src/pack/config/monitoring/prometheus.conf

See :externalref:`the metrics reference <reference-metrics>` for the full list of metrics exported.

Deprecated Reporters
~~~~~~~~~~~~~~~~~~~~

Other reporters (jmx, graphite, and csv) are supported, but they are deprecated. Any such reporter should be migrated to `Prometheus <https://prometheus.io>`__.


JMX-based reporting (for testing purposes only) can be enabled using:

::

    canton.monitoring.metrics.reporters = [{ type = jmx }]

Additionally, metrics can be written to a file:

::

    canton.monitoring.metrics.reporters = [{
      type = jmx
    }, {
      type = csv
      directory = "metrics"
      interval = 5s // default
      filters = [{
        contains = "canton"
      }]
    }]


or reported via Graphite (to Grafana) using:

::

    canton.monitoring.metrics.reporters = [{
      type = graphite
      port = 2003
      interval = 30s // default
      filters = [{
        contains = "canton"
      }]
    }]


When using the ``graphite`` or the ``csv`` reporter, Canton periodically evaluates all metrics matching the given filters. Filter for only those metrics that are relevant to you.
