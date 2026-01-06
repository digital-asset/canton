..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _reference-json-api-openapi:

JSON Ledger API OpenAPI definition
##################################

Below there is copy of :download:`OpenAPI<api/openapi.yaml>` specification of **JSON Ledger API**.

When Canton is started with the `JSON Ledger API` api enabled, the same specification is available under `http://<host>:<port>/docs/openapi`. In such case it also reflects node specific customizations (such as endpoints prefix).

This specification covers regular HTTP/S endpoints - for streaming endpoints (websockets) please navigate to :ref:`reference-json-api-asyncapi`.

Properties without type or of type: `{}` (any Json value) are Daml values as defined in the Daml template and formatted according to :ref:`reference-json-lf-value-specification`.

.. note::
   OpenAPI code generators are of various quality. Many code generators (for languages/frameworks) do not fully support OpenAPI 3.0.3 specifications, and those that support often contain bugs.
   We regularly test:

   * `java` generator from https://openapi-generator.tech
   * OpenAPI fetch for `typescript` https://openapi-ts.dev/openapi-fetch/

.. note:: Due to technical constraints, some fields are incorrectly marked as required, even though they can be omitted in JSON Ledger API. Refer to the component descriptions for accurate details.
    This will be addressed in a future update.

..
    There is an OpenAPI extension for Sphinx (sphinxcontrib.openapi), but it did not work on our file (probably due to bugs).
    The split was needed to avoid the file being too large for PDF generation.

.. literalinclude:: api/openapi.yaml
  :language: yaml
  :lines: 1-1000

.. literalinclude:: api/openapi.yaml
  :language: yaml
  :lines: 1001-2000

.. literalinclude:: api/openapi.yaml
  :language: yaml
  :lines: 2000-
