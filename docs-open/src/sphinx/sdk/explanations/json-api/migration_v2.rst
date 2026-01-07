..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _json-api_migration:

JSON Ledger API Migration to V2 guide
#####################################

Migration guide for existing  JSON Ledger API V1 users.

JSON Ledger API V2 is a mirror of a gRPC Ledger API, thus it diverges substantially in some
aspects from V1 API.


Some v1 endpoints have simple corresponding v2 endpoints, while some need more calls or extra processing in order to simulate v1 behavior.

Chapters below only point out the endpoints that should be used in `v2`.

 Always  consult :ref:`reference-json-api-openapi` for exact details and usage.

Create a New Contract
*********************

- v1 endpoint: ``POST`` ``/v1/create``
- v2 equivalent:  ``POST`` ``/v2/commands/submit-and-wait``

Put ``CreateCommand: payload`` as ``JsCommands/commands`` element.

There are also alternate endpoints available, such as:
- ``/v2/commands/submit-and-wait-for-transaction``
- ``/v2/commands/submit-and-wait-for-transaction-tree``

They might be used instead of ``/v2/commands/submit-and-wait`` to get more data about processed transactions.

Create a Contract with a Command ID
***********************************

- v1 endpoint: ``POST`` ``/v1/create``
- v2 equivalent: ``POST`` ``/v2/commands/submit-and-wait``

Same as Create a New Contract
Put ``commandId`` as part of JsCommand object.


Exercise by Contract ID
***********************

- v1 endpoint : ``POST`` ``/v1/exercise``
- v2 equivalent: ``POST`` ``/v2/commands/submit-and-wait``

Put ``ExersiseCommand: <payload>`` as ``JsCommands/commands`` element.

Check :ref:`reference-json-api-openapi` for details.


Exercise by Contract Key
************************

- v1 endpoint: ``POST`` ``/v1/exercise``
- v2 equivalent: ``POST`` ``/v2/commands/submit-and-wait``

Put ``ExerciseCommand: <payload>`` as ``JsCommands/commands`` element.

Check :ref:`reference-json-api-openapi` for details.


Create and Exercise in the Same Transaction
*******************************************

- v1 endpoint:  ``POST`` ``/v1/create-and-exercise``
- v2 equivalent: ``POST`` ``/v2/commands/submit-and-wait``

Put ``CreateAndExerciseCommand: <payload>`` as ``JsCommands/commands`` element.

Check :ref:`reference-json-api-openapi` for details.


Fetch Contract by Contract ID
*****************************

- v1 endpoint: ``POST`` ``/v1/fetch``
- v2 equivalent: ``POST`` ``/v2/events/events-by-contract-id``

Fetch Contract by Key
*********************

- v1 endpoint: ``POST`` ``/v1/fetch``
- v2 equivalent: no direct replacement

Contract keys feature is not available in ``canton 3.3``.


Get All Active Contracts
************************

- v1 endpoint: ``GET`` ``/v1/query``
- v2 equivalent: ``/v2/state/active-contracts``, if you do not have desired ledger offset (``activeAtOffset``), obtain it via a call to ``/v2/state/ledger-end``.

There is also a streaming version of this endpoint in V2, which uses websockets. It provides better scalability for large results.

Get All Active Contracts Matching a Given Query
***********************************************

- URL:  ``GET`` `/v1/query``
- v2: no direct replacement

    - Option 1: use ``/v2/state/active-contracts`` and query result in client code

    - Option 2: use PQS (suitable for more demanding use cases)


Fetch Parties by Identifiers
****************************

- v1 endpoint: ``POST`` ``/v1/parties``
- v2 equivalent: ``GET`` ``/v2/parties/<party-id>``



Fetch All Known Parties
***********************

- v1 endpoint: ``GET`` ``/v1/parties``
- v2 equivalent: ``GET`` ``/v2/parties``


Allocate a New Party
********************

- v1 endpoint: ``POST`` ``/v1/parties/allocate``
- v2 equivalent: ``POST`` ``/v2/parties``



Create a New User
*****************

- v1 endpoint: ``POST`` ``/v1/user/create``
- v2 equivalent:  ``POST``  ``/v2/users``


Get Authenticated User Information
**********************************

- v1 endpoint: ``GET`` ``/v1/user``
- v2 equivalent:  ``GET`` ``/v2/users/<user-id>``



Get Specific User Information
*****************************

- v1 endpoint: ``POST`` ``/v1/user``
- v2 equivalent:  ``GET`` ``/v2/users/<user-id>``



Delete Specific User
********************

- v1 endpoint: ``/v1/user/delete``
- v2 equivalent: ``DELETE`` ``/v2/users/<user-id>``

List Users
**********

- v1 endpoint: ``GET`` ``/v1/users``
- v2 equivalent: ``GET`` ``/v2/users``

Grant User Rights
*****************

- v1 endpoint: ``POST`` ``/v1/user/rights/grant``
- v2 equivalent: ``POST``  ``/v2/users/<user-id>/rights``

Revoke User Rights
******************

- v1 endpoint: ``POST`` ``/v1/user/rights/revoke``
- v2 equivalent: ``PATCH`` ``/v2/users/<user-id>/rights``

List Authenticated User Rights
******************************

- v1 endpoint: ``GET`` ``/v1/user/rights``
- v2 equivalent: ``GET`` ``/v2/users/<user-id>/rights``

List Specific User Rights
*************************

- v1 endpoint:  ``POST`` ``/v1/user/rights``
- v2 equivalent: ``GET`` ``/v2/users/<user-id>/rights``


List All DALF Packages
**********************


- v1 endpoint: ``GET`` ``/v1/packages``
- v2 equivalent: ``GET`` ``/v2/packages``

Download a DALF Package
***********************

- v1 endpoint: ``GET`` ``/v1/packages/<package ID>``
- v2 equivalent: ``GET`` ``/v2/packages/<package-ID>``

Upload a DAR File
*****************

- v1 endpoint: ``POST`` ``/v1/packages``
- v2 equivalent: ``POST`` ``/v2/packages``


Metering Report
***************

- v1 endpoint: ``/v1/metering-report``
- v2: no direct replacement, metring has been removed


Streaming API
*************


Contracts Query Stream
======================

- v1 endpoint: ``websocket`` ``/v1/stream/query``
- v2 equivalent: ``websocket`` ``/v2/state/active-contracts``

Fetch by Key Contracts Stream
=============================

- v1 endpoint: ``websocket`` ``/v1/stream/fetch``
- v2: no direct replacement

    - Option 1: Use PQS
    - Option 2: Use ``/v2/state/active-contracts`` with client site filtering (for infrequent queries).


