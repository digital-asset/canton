..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _json-api-configuration:

JSON Ledger API Configuration
=============================

The configuration of the JSON Ledger API is similar to the gRPC Ledger API configuration. The corresponding parameter
group starts with ``http-ledger-api``.

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/ledger-api-json.conf

The JSON Ledger API is enabled by default. If you want to disable it, set the `enabled` field to `false`.

JSON Ledger API Endpoint
~~~~~~~~~~~~~~~~~~~~~~~~

Configure the JSON API endpoint by adding the ``server`` parameter group.

- Specify the ``address`` to indicate the IP address to listen on.
- Specify the ``port`` to indicate the port.
- Specify ``port-file``, if you want the Participant Node to write a text file with the port on which the JSON Ledger API
  is listening. This is useful as a tool to synchronize the start-up of dependent services, as the Participant Node only
  writes the file once the JSON Ledger API is fully initialized.
- Specify ``path-prefix`` if you want the JSON Ledger API content to be served on a customized path instead of ``/``.

Websockets
~~~~~~~~~~

The endpoints that result in large data sets (``/v2/commands/completions``, ``/v2/state/active-contracts``,
``/v2/updates``, ``/v2/updates/flats``, and ``/v2/updates/trees``) come in two flavors:

- a websocket capable of returning an arbitrarily long result set in a stream
- a HTTP post request returning a limited list of results

The latter type of requests can be parameterized in the Participant Node's config.

- Set the ``http-list-max-elements-limit`` to specify the maximum number of returned rows.
- Set the ``http-list-wait-time`` to specify the number of milliseconds of idle time to wait before sending result
  This parameter is needed in case of open-ended streams such as completions. JSON Ledger API reads from the gRPC stream,
  and when there are no new elements for "wait" milliseconds -> the result is sent to the client.

Daml definitions service
~~~~~~~~~~~~~~~~~~~~~~~~

You can enable the experimental ``Daml Definitions Service`` by setting the ``daml-definitions-service-enabled``
parameter to ``true``.
