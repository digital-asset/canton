..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _references_json-api:

JSON Ledger API references
###############################

You can use :ref:`openapi.yaml<reference-json-api-openapi>` and :ref:`asyncapi.yaml<reference-json-api-asyncapi>` files that describe the JSON Ledger API and the streaming API, respectively.


Location of JSON Ledger API files
-----------------------------------------------

If you have a running instance of Canton with JSON Ledger API enabled, you can download definitions using links:

* **http://<host>:<port>/docs/openapi**

* **http://<host>:<port>/docs/asyncapi**

where **<host>** and **<port>** are the host and port of your Canton instance. Check  :ref:`configuration<reference-json-api-configuration>`.


For a general introduction to the JSON Ledger API , see the following sections :ref:`json-api`.

Daml values
-----------

Some fields (for instance `createArguments` in `CreateCommand`) do not specify any type.

This means the actual type is determined by the Daml model. See :ref:`Daml type conversion<reference-json-lf-value-specification>` for more details.


