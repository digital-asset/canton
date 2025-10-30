..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _reference-json-api-asyncapi:

JSON Ledger API AsyncAPI definitions (websocket access)
#######################################################

This section contains a copy of the :download:`AsyncAPI<api/asyncapi.yaml>` specification of **JSON Ledger API**.

If you start Canton with the JSON Ledger API enabled, this specification is available under http://<host>:<port>/docs/asyncapi. In this case, it also reflects node-specific customizations (such as endpoints prefix).

The specification covers streaming (websockets) endpoints - for regular endpoints (HTTP) please see :ref:`reference-json-api-openapi`

Properties of type: `{}` (any Json value) are Daml values as defined in the Daml template and formatted according to :ref:`reference-json-lf-value-specification`.

.. literalinclude:: api/asyncapi.yaml
  :language: yaml
