..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. |br| raw:: html

   <br />

.. _reference-json-lf-value-specification:

Daml-LF JSON encoding
#####################

Daml-LF values are represented as JSON values in the JSON Ledger API.
This representation is used whenever the `JSON Ledger API` expects a `Daml` record or value, such as in the `createArguments` field of the `CreateCommand` component.
The conversion from Daml values to JSON also occurs when a value is returned from the `JSON Ledger API`.
For more information about `Daml` types, see :subsiteref:`daml-ref-built-in-types`.

.. csv-table:: Daml LF JSON encoding
   :file: daml-lf-json-encoding.csv
   :widths: 20, 20, 60
   :header-rows: 1
   :delim: ;
   :quote: $
   :escape: ^


.. rubric::  Footnotes

.. [#js_number] Note that JSON numbers is enough to represent all
    Decimals. Strings are preferred because many languages
    (most notably JavaScript) use IEEE Doubles to express JSON numbers, and
    IEEE Doubles cannot express Daml-LF Decimals correctly. For convenience,
    during parsing we accept both JSON numbers and strings.

.. [#timestamp] Timestamp parsing is more flexible and uses the format
    ``yyyy-mm-ddThh:mm:ss(\.s+)?Z``, i.e. it's OK to omit the microsecond part
    partially or entirely, or have more than 6 decimals. Sub-second data beyond
    microseconds will be dropped. The UTC timezone designator must be included. The
    rationale behind the inclusion of the timezone designator is minimizing the
    risk that users pass in local times.

