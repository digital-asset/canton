..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _json-api:

JSON Ledger API Service V2
##########################

.. note::
    THIS SECTION NEEDS TO MOVE TO sdk/sdlc-howtos/applications/integrate/http-json

This section describes the new JSON Ledger API Service that is a part of Canton 3.x. If you are looking for the legacy JSON Ledger API, please navigate to `JSON Ledger API V1 <https://docs.daml.com/app-dev/json-api.html>`

The **JSON Ledger API** provides a way to interact with a ledger using HTTP and JSON instead of the gRPC
`the gRPC Ledger API <https://docs.daml.com/app-dev/ledger-api.html>`__.
The JSON Ledger API provides almost all the functionality available in the gRPC Ledger API, although the HTTP protocol imposes some minor limitations.

The full reference consists of:
    * :ref:`openapi<reference-json-api-openapi>`
    * :ref:`asyncapi<reference-json-api-asyncapi>`
    * `the gRPC Ledger API <https://docs.daml.com/app-dev/ledger-api.html>`__.

We recommend using OpenAPI/AsyncAPI specification to generate a client for your technology (Java, typescript).


We welcome feedback about the JSON Ledger API on
`our issue tracker <https://github.com/digital-asset/daml/issues/new/choose>`_, or
`on our forum <https://discuss.daml.com>`_.


Prerequisites
****************

It is assumed that you know the basics of HTTP Protocol, HTTP methods, HTTP request and response headers (including authorization), JSON format, and WebSockets.

In order to test JSON Ledger API you need a tool such as:
 * command line ``curl``
 * postman `<https://www.postman.com>`


Some tools enable testing of WebSockets; one is, for instance: ``wscat``.

Run the JSON Ledger API
***********************

.. _reference-json-api-configuration:

Prepare canton configuration
============================

.. code-block:: none

    canton {
      participants {
        participant1 {
          storage {
            type = memory
          }
          admin-api {
            port = 14012
          }
          http-ledger-api {
            port = 8080
         }
       }
      }
    }

Save config as (for instance) ``json_enabled.conf``

.. note:: in canton 3.1 and earlier `http-ledger-api` is named `http-ledger-api-experimental`.


Start canton
===================

You can run the JSON Ledger API alongside any ledger exposing the gRPC Ledger API you want. If you don't have an existing ledger, you can start canton with the config as below:

.. code-block:: shell

    bin/canton -c json_enabled.conf


#. Ensure that the canton console is started.

#. Check that JSON Ledger API is running:

    In terminal use curl to get openapi documentation

    .. code-block:: shell

        curl localhost:8080/docs/openapi

    Alternatively open the web browser and navigate to address:

        `<http://localhost:8080/docs/openapi>`_

    You should see yaml file contents that starts with:

    .. code-block:: none

        openapi: 3.1.0
        info:
            title: JSON Ledger API HTTP endpoints


    Congratulations, You have successfully started JSON Ledger API.


.. _start-http-service:

.. note:: Your JSON Ledger API service should never be exposed to the Internet. When running in production the JSON Ledger API should be behind a `reverse proxy, such as via NGINX <https://docs.nginx.com/nginx/admin-guide/web-server/reverse-proxy/>`_.


JSON Ledger API Advanced configuration
======================================

The full set of configurable options that can be specified via config file is listed below:

.. code-block:: none

    {
      server {
        //IP address that JSON Ledger API service listens on. Defaults to 127.0.0.1.
        address = "127.0.0.1"
        //JSON Ledger API service port number. A port number of 0 will let the system pick an ephemeral port.
        port = 7575
        //Prefix added to all JSON endpoints.
        path-prefix = "example/prefix"
      }
      websocket-config {
                    //Maximum number of elements returned when using the HTTP POST alternative.
                    http-list-max-elements-limit = 1024
                    //Wait time for new elements before returning the list via the HTTP POST alternative.
                    http-list-wait-time = "1s"
      }

    }


.. _json-api-access-tokens:

Access Tokens
=============

Each request to the JSON Ledger API Service *must* come with an access token (``JWT``). This also includes development setups using an unsecured sandbox.
The JSON Ledger API Service *does not*
hold on to the access token, which will be only used to fulfill the request it came along with. The same token will be used
to issue the request to the Ledger API.
The only exceptions are documentation endpoints, which do not strictly require tokens.

For  a reference on the JWT Tokens we use, please read:
 :subsiteref:`Authorization <authorization>`.

For a ledger without authorization, e.g., the default configuration of Daml Sandbox, you can use `https://jwt.io <https://jwt.io/#debugger-io?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJzYW5kYm94IiwiYXBwbGljYXRpb25JZCI6ImZvb2JhciIsImFjdEFzIjpbIkFsaWNlIl19fQ.1Y9BBFH5uVz1Nhfmx12G_ECJVcMncwm-XLaWM40EHbY>`_ (or the JWT library of your choice) to generate your
token.  You can use an arbitrary secret here. The default "header" is fine.  Under "Payload", fill in:

.. code-block:: json

    {
      "aud": [],
      "exp": null,
      "iss": null,
      "scope": "ExpectedTargetScope",
      "sub": "actAs-Alice::122099e6b7c92e12eb026f483f59a73e48149dc6e630a4f4f8fb95b8d269219b356c"
    }

The value after ``actAs`` is specified as a list and you provide it with the party that you want to use,
such as in the example above which uses ``Alice`` for a party. ``actAs`` may include more than just one party
as the JSON Ledger API supports multi-party submissions.

The party should reference an already allocated party.
In canton console a party can be allocated using:

.. code-block:: shell

    val alice = participant1.parties.enable("Alice")
    alice.toProtoPrimitive //this will display a full party Id


Then the "Encoded" box should have your **token**, ready for passing to
the service as described in the following sections.

Alternatively, here is a token you can use for testing:

.. code-block:: json

    {
        "aud": [],
        "exp": null,
        "iss": null,
        "scope": "ExpectedTargetScope",
        "sub": "participant_admin"
    }

.. code-block:: none

    eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOltdLCJleHAiOm51bGwsImlzcyI6bnVsbCwic2NvcGUiOiJFeHBlY3RlZFRhcmdldFNjb3BlIiwic3ViIjoicGFydGljaXBhbnRfYWRtaW4ifQ.8bABNm1t718TuJXwRQOF2gXOclrL38t0uCmWkIT7Pcg


Auth via HTTP
^^^^^^^^^^^^^

Set HTTP header ``Authorization: Bearer <paste-jwt-here>``


Example header:

.. code-block:: none

    Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJIVFRQLUpTT04tQVBJLUdhdGV3YXkiLCJhY3RBcyI6WyJBbGljZSJdfX0.34zzF_fbWv7p60r5s1kKzwndvGdsJDX-W4Xhm4oVdpk

Example using ``curl``

.. code-block:: bash

    curl -X 'POST' \
        'http://localhost:8080/v2/users' \
        -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOltdLCJleHAiOm51bGwsImlzcyI6bnVsbCwic2NvcGUiOiJFeHBlY3RlZFRhcmdldFNjb3BlIiwic3ViIjoicGFydGljaXBhbnRfYWRtaW4ifQ.8bABNm1t718TuJXwRQOF2gXOclrL38t0uCmWkIT7Pcg' \
        -H 'accept: application/json' \
        -H 'Content-Type: application/json' \
        -d @body_create_user.json



Auth via WebSockets
^^^^^^^^^^^^^^^^^^^

WebSocket clients support a "subprotocols" argument (sometimes simply
called "protocols"); this is usually in a list form but occasionally in
comma-separated form.  Check the documentation for your WebSocket library of
choice for details.

For HTTP JSON requests, you must pass two subprotocols:

- ``daml.ws.auth``
- ``jwt.token.paste-jwt-here``

Example:

.. code-block:: none

    jwt.token.eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwczovL2RhbWwuY29tL2xlZGdlci1hcGkiOnsibGVkZ2VySWQiOiJNeUxlZGdlciIsImFwcGxpY2F0aW9uSWQiOiJIVFRQLUpTT04tQVBJLUdhdGV3YXkiLCJhY3RBcyI6WyJBbGljZSJdfX0.34zzF_fbWv7p60r5s1kKzwndvGdsJDX-W4Xhm4oVdpk


Example using ``wscat``:

.. code-block:: none

    wscat -c http://localhost:8080/v2/state/active-contracts \
        -s "jwt.token.eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOltdLCJleHAiOm51bGwsImlzcyI6bnVsbCwic2NvcGUiOiJFeHBlY3RlZFRhcmdldFNjb3BlL1dpdGgtRGFzaC9BbmRfVW5kZXJzY29yZSIsInN1YiI6ImFjdEFzLUFsaWNlOjoxMjIwOTllNmI3YzkyZTEyZWIwMjZmNDgzZjU5YTczZTQ4MTQ5ZGM2ZTYzMGE0ZjRmOGZiOTViOGQyNjkyMTliMzU2YyJ9.FmJoP6eK2Yg6CAmO2G0SYosMS4IOai_6HlvO1siYNUA" \
        -s "daml.ws.auth"

.. _json-error-codes:

HTTP Status Codes
*****************

The **JSON Ledger API** reports errors using standard HTTP status codes. It divides HTTP status codes into 3 groups indicating:

1. success (200)
2. failure due to a client-side problem (400, 401, 403)
3. failure due to a server-side problem (500)

Most common HTTP status codes are:

- 200 - OK
- 400 - Bad Request (Client Error)
- 401 - Unauthorized, authentication required
- 403 - Forbidden, insufficient permissions (TODO)
- 500 - Internal Server Error


JSON Ledger API Errors
**********************

When the gRPC Ledger API returns an error code, the JSON Ledger API maps it to one of the above codes according to `the official gRPC to HTTP code mapping <https://cloud.google.com/apis/design/errors#generating_errors>`_.

If a client's HTTP GET or POST request reaches an API endpoint, the corresponding response will always contain a JSON object. Either an expected message (corresponding to endpoint) or an  ``error`` object specified as in the example below:

.. code-block:: none

    {
    "cause" : "The submitted request has invalid arguments: Cannot unassign contract `ContractId(00a7feb291fe1be6289c4d77f3a432b623083ed3f49bf8535f8571aa8bdf68b647ca10122006207a71ac6ed62dae27429b43e456dfbbc411a49d67b7e0067bef4c914c6e8a)`: source and target domains are the same",
    "code" : "INVALID_ARGUMENT",
    "context" : {
        "category" : "8",
         "definite_answer" : "false",
          "participant" : "participant1",
          "test" : "JsonV2Tests",
         "tid" : "99e67812af315256ef9a02a9fdb94646"
      },
    "correlationId" : null,
    "definiteAnswer" : null,
    "errorCategory" : 8,
    "grpcCodeValue" : 3,
    "resources" : [ ],
    "retryInfo" : null,
    "traceId" : "99e67812af315256ef9a02a9fdb94646"
    }

Where:

- ``cause`` --  a textual message containing readable error reason,
- ``code`` -- a LedgerAPI error code,
- ``context`` -- a Ledger API context of an error,
- ``traceId`` -- telemetry tracing id
- ``grpcCodeValue`` and ``errorCategory``  - defined in Canton Error Codes.


See The Ledger API error codes (Canton Error Codes Reference) for more details about error codes from Ledger API.


WebSockets Errors
========================================

In the case of websockets an error might be delivered as a frame. Each incoming frame can either be a correct response (corresponding to the endpoint definition) or an error frame in the format above.




Example of successful Response, HTTP Status: 200 OK
===================================================

- Content-Type: ``application/json``
- Content:

.. code-block:: none

    {
      "partyDetails": {
        "party": "Carol:-_ a9c80151-002c-445e-9fae-3caf111e3ac4::122099e6b7c92e12eb026f483f59a73e48149dc6e630a4f4f8fb95b8d269219b356c",
        "isLocal": true,
        "localMetadata": {
          "resourceVersion": "0",
          "annotations": {}
        },
        "identityProviderId": ""
      }
    }


Create a New Contract
*********************

To create an ``Iou`` contract from the `Quickstart guide <https://docs.daml.com/app-dev/bindings-java/quickstart.html>`__:

.. literalinclude:: ./daml/Iou.daml
  :language: daml
  :start-after: -- BEGIN_IOU_TEMPLATE_DATATYPE
  :end-before: -- END_IOU_TEMPLATE_DATATYPE

.. _create-request:

HTTP Request
============

- URL,  one of:
    - ``/v2/commands/submit-and-wait-for-transaction-tree``
    - ``/v2/commands/submit-and-wait-for-transaction``
    -  ``/v2/commands/submit-and-wait``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "actAs": [
        "Alice::1220ceff195fd59bbc357415ce25d7a6edb95475b134b1bbb6d70c8fa3dd3f8f32ad"
      ],
      "userId": "app1",
      "commandId": "somecommandid2",
      "commands": [
        {
          "CreateCommand": {
            "createArguments": {
              "observers": [],
              "issuer": "Alice::1220ceff195fd59bbc357415ce25d7a6edb95475b134b1bbb6d70c8fa3dd3f8f32ad",
              "amount": "999.99",
              "currency": "USD",
              "owner": "Alice::1220ceff195fd59bbc357415ce25d7a6edb95475b134b1bbb6d70c8fa3dd3f8f32ad"
            },
            "templateId": "#model-tests:Iou:Iou"
          }
        }
      ]
    }



Where:

- ``template_id`` is the contract template identifier which has the following
  two formats: ``#<package name>:<module>:<entity>`` or ``<package
  ID>:<module>:<entity>``.

  The package name format works with the smart contract upgrading feature so
  that contracts with different package IDs but the same package name can be
  used in a uniform manner.

  Using the package ID format refers to a single package.



- ``createArguments`` field contains contract fields as defined in the Daml template and formatted according to :ref:`reference-json-lf-value-specification`.
- ``commandId`` -- optional field, a unique string identifying the command.
- ``actAs`` -- a non-empty list of parties, overriding the set from the JWT user; must be a subset of the JWT user's set.

.. note::  Only a subset of the possible fields is shown in the example. Refer to the OpenAPI specification for additional options.

.. _example_response: (in case of 1/v2/commands/submit-and-wait-for-transaction-tree)

HTTP Response
=============

 The response will differ according to endpoint URL chosen to send transaction. Example below assumes ``/v2/commands/submit-and-wait-for-transaction-tree``.

- Content-Type: ``application/json``
- Content:

.. code-block:: json

     {
       "transactionTree": {
         "updateId": "12208230390f6af5a6e85e70af2f0ad3cdb4f2df2f1e41778a58ce888e128d2a1245",
         "commandId": "somecommandid21",
         "workflowId": "",
         "effectiveAt": "2025-01-27T15:30:36.430940Z",
         "offset": 18,
         "eventsById": {
           "0": {
             "CreatedTreeEvent": {
               "value": {
                 "offset": 18,
                 "nodeId": 0,
                 "contractId": "005f954f6759aa04b0998676689610fc1d84c5d31a46819f7bfdd7233ecd7e18b3ca1012207866cf05034eddcc7b0619ab06a46514b21732a0da6404f3f2fbb90872ed6289",
                 "templateId": "19b9fc297d4649bab9fd0eeb3ce23da763cb9ed7dfaedeb1281ac7d7f65a7883:Iou:Iou",
                 "contractKey": null,
                 "createArgument": {
                   "issuer": "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55",
                   "owner": "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55",
                   "currency": "USD",
                   "amount": "999.9900000000",
                   "observers": []
                 },
                 "createdEventBlob": "",
                 "interfaceViews": [],
                 "witnessParties": [
                   "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55"
                 ],
                 "signatories": [
                   "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55"
                 ],
                 "observers": [],
                 "createdAt": "2025-01-27T15:30:36.430940Z",
                 "packageName": "model-tests"
               }
             }
           }
         },
         "rootNodeIds": [
           0
         ],
         "synchronizerId": "da1::122002b3938ee8f7f0a757c5db698fc6020352717ee53da9023485f8b9e0dc2ea360",
         "traceContext": {
           "traceparent": "00-bfc45078ca09495a4101b227f4c36bef-6471ff18d20230eb-01",
           "tracestate": null
         },
         "recordTime": "2025-01-27T15:30:37.291866Z"
       }
     }

Where:

- ``eventsById`` contains ledger events with details of created contract(s),



Exercise by Contract ID
***********************

The JSON command below, demonstrates how to exercise an ``Iou_Transfer`` choice on an ``Iou`` contract:

.. literalinclude:: ./daml/Iou.daml
  :language: daml
  :start-after: -- BEGIN_IOU_TEMPLATE_TRANSFER
  :end-before: -- END_IOU_TEMPLATE_TRANSFER

HTTP Request
============

- URL,  one of:
    - ``/v2/commands/submit-and-wait-for-transaction-tree``
    - ``/v2/commands/submit-and-wait-for-transaction``
    -  ``/v2/commands/submit-and-wait``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

     {
       "actAs": [
         "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55"
       ],
       "userId": "app1",
       "commandId": "somecommandid2",
       "commands": [
         {
           "ExerciseCommand": {
             "choice": "Iou_Transfer",
             "choiceArgument": {
               "newOwner": "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55"
             },
             "contractId": "005f954f6759aa04b0998676689610fc1d84c5d31a46819f7bfdd7233ecd7e18b3ca1012207866cf05034eddcc7b0619ab06a46514b21732a0da6404f3f2fbb90872ed6289",
             "templateId": "#model-tests:Iou:Iou"
           }
         }
       ]
     }


Where:

- ``templateId`` -- contract template or interface identifier, same as in :ref:`create request <create-request>`,
- ``choiceInterfaceId`` -- *optional* template or interface that defines the choice, same format as ``templateId``,
- ``contractId`` -- contract identifier, the value from the  :brokenref:`create response <create-response>`,
- ``choice`` -- Daml contract choice, that is being exercised,
- ``argument`` -- contract choice argument(s).


.. _exercise-response: (using ``/v2/commands/submit-and-wait``)

HTTP Response
=============

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "completionOffset" : 11,
      "updateId" : "12205cec9cccf807dd95df3c0ab270bd286ef1bc9b84aa26f7ecf1180248aaa9d070"
    }

Where:

    + ``update_id``  the id of the transaction that resulted from the submitted command.
    + ``completionOffset`` is the ledger offset of the transaction containing the exercise's ledger changes.

Create and Exercise in the Same Transaction
*******************************************

This command allows creating a contract and exercising a choice on the newly created contract in the same transaction.

HTTP Request
============

- URL,  one of:
    - ``/v2/commands/submit-and-wait-for-transaction-tree``
    - ``/v2/commands/submit-and-wait-for-transaction``
    -  ``/v2/commands/submit-and-wait``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "actAs" : [ "Alice::1220870d67dba562b868d0256cb2968dbc59a7a6bdbc4b82ee623ff96c8cda3afb9a" ],
      "userId" : "defaultuser2",
      "commandId" : "somecommandid3",
      "commands" : [ {
        "CreateAndExerciseCommand" : {
          "choice" : "Iou_Transfer",
          "createArguments" : {
                      "observers" : [ ],
                      "issuer" : "Alice::1220df15d08ac34527e46492a6ee48a723e3d02ed3ec20a05ebf64be47173f24407f",
                      "amount" : "999.99",
                      "currency" : "USD",
                      "owner" : "Alice::1220df15d08ac34527e46492a6ee48a723e3d02ed3ec20a05ebf64be47173f24407f"
                    },
          "choiceArgument" : {
            "newOwner" : "Alice::1220870d67dba562b868d0256cb2968dbc59a7a6bdbc4b82ee623ff96c8cda3afb9a"
          },
          "templateId" : "#model-tests:Iou:Iou"
        }
      } ]
    }

Where:

- ``templateId`` -- the initial contract template identifier, in the same format as in the :ref:`create request <create-request>`,
- ``payload`` -- the initial contract fields as defined in the Daml template and formatted according to :ref:`reference-json-lf-value-specification`,
- ``choiceInterfaceId`` -- *optional* template or interface that defines the choice, same format as ``templateId``,
- ``choice`` -- Daml contract choice, that is being exercised,
- ``argument`` -- contract choice argument(s).

HTTP Response
=============

Please note that the response below is for a consuming choice, it contains:

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "transaction" : {
        "command_id" : "somecommandid",
        "domainId" : "domain1::12208f22808f6fe77f8689d0d565488e34e9a3abb2eaf37270f6e5b2b2073894db9f",
        "effectiveAt" : "1970-01-01T00:00:00Z",
        "events" : [ {
          "CreatedEvent" : {
            "contractId" : "00811e868ffe78638ecf14b8d69a5df6eafd8b16f13a431117ee26981cc39f40e1ca101220c15b891d2cac1287d5067594c81f60b722141204f63f2a27c3328a70467c5018",
            "contractKey" : null,
            "createArgument" : {
              "iou" : {
                "amount" : "999.9900000000",
                "currency" : "USD",
                "issuer" : "Alice_ffe0d699-2978-42d7-a315-b7faec7ba95a::1220d7b8bc76f04500216f07e1576da75db0e811ea1e01d5688140816823615a34bf",
                "observers" : [ ],
                "owner" : "Alice_ffe0d699-2978-42d7-a315-b7faec7ba95a::1220d7b8bc76f04500216f07e1576da75db0e811ea1e01d5688140816823615a34bf"
              },
              "newOwner" : "Alice_ffe0d699-2978-42d7-a315-b7faec7ba95a::1220d7b8bc76f04500216f07e1576da75db0e811ea1e01d5688140816823615a34bf"
            },
            "createdAt" : "1970-01-01T00:00:00Z",
            "createdEventBlob" : "",
            "eventId" : "#1220ad3e6509fa8977478aa49cf3261b376dd55b06f6409e3ec59553272c89d3616c:2",
            "interfaceViews" : [ ],
            "nodeId" : 2,
            "observers" : [ ],
            "offset" : 12,
            "packageName" : "model-tests",
            "signatories" : [ "Alice_ffe0d699-2978-42d7-a315-b7faec7ba95a::1220d7b8bc76f04500216f07e1576da75db0e811ea1e01d5688140816823615a34bf" ],
            "templateId" : "a36cfff1da2f15da3d760c872860bf03140aa699f73b57b639fd40dfe8156cfe:Iou:IouTransfer",
            "witnessParties" : [ "Alice_ffe0d699-2978-42d7-a315-b7faec7ba95a::1220d7b8bc76f04500216f07e1576da75db0e811ea1e01d5688140816823615a34bf" ]
          }
        } ],
        "offset" : 12,
        "recordTime" : "1970-01-01T00:00:00.000042Z",
        "traceContext" : {
          "traceparent" : "00-d84d23ff01031adbcf76f19525fa62fb-003327aa6c8ede2d-01",
          "tracestate" : null
        },
        "updateId" : "1220ad3e6509fa8977478aa49cf3261b376dd55b06f6409e3ec59553272c89d3616c",
        "workflowId" : ""
      }
    }

Get All Active Contracts
************************

List all currently active contracts for all known templates.

.. note:: You can only query active contracts with the ``/v2/state/active-contracts`` endpoint. Archived contracts (those that were archived or consumed during an exercise operation) will not be shown in the results.

HTTP Websocket Request
======================

- URL: ``/v2/state/active-contracts``
- Protocol: ``Websocket``
- Content:

Send a single query frame:

.. code-block:: json

    {
        "filter":
        {
            "filtersByParty":{},
            "filtersForAnyParty":
                {"cumulative":  [
                    {"identifierFilter":
                        {"WildcardFilter":
                            {"value":
                                {"includeCreatedEventBlob":true}
                                }
                            }
                        }
                    ]
                }
            },
        "verbose":false,
        "activeAtOffset":23
   }

.. note:: be careful and do enter a proper value for activeAtOffset

Example Response frames
=======================

The response might look like an example below:

.. code-block:: json

    {
      "workflowId": "",
      "contractEntry": {
        "JsActiveContract": {
          "createdEvent": {
            "offset": 23,
            "nodeId": 1,
            "contractId": "00afee8d924751d4677ae7f09ed7c38c84b9e8601bb2c75bc30f832f866896755dca101220b62bec39ca9352f599cc432bbca1969a33d368a0ee4cdad56093226f27103dc2",
            "templateId": "19b9fc297d4649bab9fd0eeb3ce23da763cb9ed7dfaedeb1281ac7d7f65a7883:Iou:IouTransfer",
            "contractKey": null,
            "createArgument": {
              "iou": {
                "issuer": "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55",
                "owner": "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55",
                "currency": "USD",
                "amount": "999.9900000000",
                "observers": []
              },
              "newOwner": "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55"
            },
            "createdEventBlob": "CgMyLjES0QQKRQCv7o2SR1HUZ3rn8J7Xw4yEuehgG7LHW8MPgy+GaJZ1XcoQEiC2K+w5ypNS9ZnMQyu8oZaaM9NooO5M2tVgkyJvJxA9whILbW9kZWwtdGVzdHMaVApAMTliOWZjMjk3ZDQ2NDliYWI5ZmQwZWViM2NlMjNkYTc2M2NiOWVkN2RmYWVkZWIxMjgxYWM3ZDdmNjVhNzg4MxIDSW91GgtJb3VUcmFuc2ZlciKiAmqfAgrLAQrIAWrFAQpPCk06S0FsaWNlOjoxMjIwNzQzOTI2YWRhMWJhNTIxNTA1OTZjMDc3MTI5OTVmNGExZDJjNDNlZmNjOWQ5ZTU2OWJiODJiMTk5OGE0NmM1NQpPCk06S0FsaWNlOjoxMjIwNzQzOTI2YWRhMWJhNTIxNTA1OTZjMDc3MTI5OTVmNGExZDJjNDNlZmNjOWQ5ZTU2OWJiODJiMTk5OGE0NmM1NQoHCgVCA1VTRAoSChAyDjk5OS45OTAwMDAwMDAwCgQKAloACk8KTTpLQWxpY2U6OjEyMjA3NDM5MjZhZGExYmE1MjE1MDU5NmMwNzcxMjk5NWY0YTFkMmM0M2VmY2M5ZDllNTY5YmI4MmIxOTk4YTQ2YzU1KktBbGljZTo6MTIyMDc0MzkyNmFkYTFiYTUyMTUwNTk2YzA3NzEyOTk1ZjRhMWQyYzQzZWZjYzlkOWU1NjliYjgyYjE5OThhNDZjNTU5Agm71LEsBgBCKgomCiQIARIgyJpshr25uq/bCHZXro6K5Wl3FyO/llMqF2W5DbrzoKAQHg==",
            "interfaceViews": [],
            "witnessParties": [
              "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55"
            ],
            "signatories": [
              "Alice::1220743926ada1ba52150596c07712995f4a1d2c43efcc9d9e569bb82b1998a46c55"
            ],
            "observers": [],
            "createdAt": "2025-01-27T15:35:50.124802Z",
            "packageName": "model-tests"
          },
          "synchronizerId": "da1::122002b3938ee8f7f0a757c5db698fc6020352717ee53da9023485f8b9e0dc2ea360",
          "reassignmentCounter": 0
        }
      }
    }


Get All Active Contracts via HTTP
*********************************

For each WebSocket endpoint, we provide an alternative HTTP POST endpoint that returns the same results synchronously.HTTP POST alternatives are less robust:
However, HTTP POST alternatives are less efficient because:

- They impose a higher load on the server.
- The result size is always limited (refer to the configuration).
- There is a slight performance overhead and delay in retrieving results.


We recommend using WebSockets for production environments, as they offer better performance and scalability. HTTP alternatives are suitable for prototyping and less demanding applications with lower data and load requirements.

HTTP Request
===============================

- URL: ``/v2/state/active-contracts``
- Protocol: ``HTTP``
- Method: ``POST``
- Content:

.. code-block:: json

    {
        "filter":
        {
            "filtersByParty":{},
            "filtersForAnyParty":
                {"cumulative":  [
                    {"identifierFilter":
                        {"WildcardFilter":
                            {"value":
                                {"includeCreatedEventBlob":true}
                                }
                            }
                        }
                    ]
                }
            },
        "verbose":false,
        "activeAtOffset":23
   }

.. note:: be careful and do enter a proper value for activeAtOffset

Example HTTP Response:
======================

.. code-block:: json

  [
    {
      "workflowId": "",
      "contractEntry": {
        "JsActiveContract": {
          "createdEvent": {
            "offset": 18,
            "nodeId": 0,
            "contractId": "0047c4cc5cf2f0bda731a172fd90f5cc592f9d3ad2e5c45450d9de1cc4aa5adb71ca101220a8faa4673f7e9c28319eb57f98aad84d0bf1395fed971f24eb486bc4f48d9828",
            "templateId": "19b9fc297d4649bab9fd0eeb3ce23da763cb9ed7dfaedeb1281ac7d7f65a7883:Iou:Iou",
            "contractKey": null,
            "createArgument": {
              "issuer": "Alice::1220b6683b7d875557055126f8b8f059d9c070ece95e7784d279a135c8c1285a4f1f",
              "owner": "Alice::1220b6683b7d875557055126f8b8f059d9c070ece95e7784d279a135c8c1285a4f1f",
              "currency": "USD",
              "amount": "999.9900000000",
              "observers": []
            },
            "createdEventBlob": "CgMyLjES7wMKRQBHxMxc8vC9pzGhcv2Q9cxZL5060uXEVFDZ3hzEqlrbccoQEiCo+qRnP36cKDGetX+YqthNC/E5X+2XHyTrSGvE9I2YKBILbW9kZWwtdGVzdHMaTApAMTliOWZjMjk3ZDQ2NDliYWI5ZmQwZWViM2NlMjNkYTc2M2NiOWVkN2RmYWVkZWIxMjgxYWM3ZDdmNjVhNzg4MxIDSW91GgNJb3UiyAFqxQEKTwpNOktBbGljZTo6MTIyMGI2NjgzYjdkODc1NTU3MDU1MTI2ZjhiOGYwNTlkOWMwNzBlY2U5NWU3Nzg0ZDI3OWExMzVjOGMxMjg1YTRmMWYKTwpNOktBbGljZTo6MTIyMGI2NjgzYjdkODc1NTU3MDU1MTI2ZjhiOGYwNTlkOWMwNzBlY2U5NWU3Nzg0ZDI3OWExMzVjOGMxMjg1YTRmMWYKBwoFQgNVU0QKEgoQMg45OTkuOTkwMDAwMDAwMAoECgJaACpLQWxpY2U6OjEyMjBiNjY4M2I3ZDg3NTU1NzA1NTEyNmY4YjhmMDU5ZDljMDcwZWNlOTVlNzc4NGQyNzlhMTM1YzhjMTI4NWE0ZjFmOS/eEk3FLAYAQioKJgokCAESIANCONtfqgCanbAYj1wMaIE3YKLDoH+lM9kT19z2h1F4EB4=",
            "interfaceViews": [],
            "witnessParties": [
              "Alice::1220b6683b7d875557055126f8b8f059d9c070ece95e7784d279a135c8c1285a4f1f"
            ],
            "signatories": [
              "Alice::1220b6683b7d875557055126f8b8f059d9c070ece95e7784d279a135c8c1285a4f1f"
            ],
            "observers": [],
            "createdAt": "2025-01-28T14:49:33.525551Z",
            "packageName": "model-tests"
          },
          "synchronizerId": "da1::1220ff7a4caf08bd4442003c65c2f351ebc15cc3a2063fd876da82e3a64a2348d30b",
          "reassignmentCounter": 0
        }
      }
    }
  ]


Fetch Parties by Identifiers
****************************

- URL: ``/v2/parties/{partyId}``
- Method: ``GET``
- Content-Type: ``application/json``
- Content:


HTTP Response
=============

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "partyDetails": [
        {
          "party": "Alice::122099e6b7c92e12eb026f483f59a73e48149dc6e630a4f4f8fb95b8d269219b356c",
          "isLocal": true,
          "localMetadata": {
            "resourceVersion": "",
            "annotations": {}
          },
          "identityProviderId": ""
        }
      ]
    }

Where

- ``identifier`` -- a stable unique identifier of a Daml party,
- ``isLocal`` -- true if party is hosted by the backing participant.

The ``result`` might contain an empty JSON array `partyDetails`.

Fetch All Known Parties
***********************

- URL: ``/v2/parties``
- Method: ``GET``
- Content: <EMPTY>

HTTP Response
=============

.. code-block:: json

    {
      "partyDetails": [
        {
          "party": "Alice::122099e6b7c92e12eb026f483f59a73e48149dc6e630a4f4f8fb95b8d269219b356c",
          "isLocal": true,
          "localMetadata": {
            "resourceVersion": "",
            "annotations": {}
          },
          "identityProviderId": ""
        },
        {
          "party": "Bob::122099e6b7c92e12eb026f483f59a73e48149dc6e630a4f4f8fb95b8d269219b356c",
          "isLocal": true,
          "localMetadata": {
            "resourceVersion": "",
            "annotations": {}
          },
          "identityProviderId": ""
        },
        {
          "party": "participant1::122099e6b7c92e12eb026f483f59a73e48149dc6e630a4f4f8fb95b8d269219b356c",
          "isLocal": true,
          "localMetadata": {
            "resourceVersion": "",
            "annotations": {}
          },
          "identityProviderId": ""
        }
      ],
      "nextPageToken": ""
    }

.. note:: `nextPageToken` is used by gRPC if there is paging enabled, in the current resource version of JSON Ledger API paging is not yet supported.


Allocate a New Party
********************

HTTP Request
============

- URL: ``/v2/parties``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "identityProviderId" : "",
      "localMetadata" : null,
      "partyIdHint" : "Carol"
    }


HTTP Response
=============

.. code-block:: json

    {
      "partyDetails": {
        "party": "Carol:-_ a9c80151-002c-445e-9fae-3caf111e3ac4::122099e6b7c92e12eb026f483f59a73e48149dc6e630a4f4f8fb95b8d269219b356c",
        "isLocal": true,
        "localMetadata": {
          "resourceVersion": "0",
          "annotations": {}
        },
        "identityProviderId": ""
      }
    }


Create a New User
*****************

HTTP Request
============

- URL: ``/v2/users``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

  {
    "rights" : [
      {
        "kind": {
          "CanActAs" : {
            "value": {
              "party" : "Bob::122099e6b7c92e12eb026f483f59a73e48149dc6e630a4f4f8fb95b8d269219b356c"
            }
          }
        }
      }
    ],
    "user" : {
      "id" : "CreatedUser1",
      "identityProviderId" : "",
      "isDeactivated" : false,
      "metadata" : null,
      "primaryParty" : ""
    }
  }


Only the userId fields in the request is required, this means that an JSON object containing only it is a valid request to create a new user.

HTTP Response
=============

.. code-block:: json

    {
      "user": {
        "id": "CreatedUser1",
        "primaryParty": "",
        "isDeactivated": false,
        "metadata": {
          "resourceVersion": "0",
          "annotations": {}
        },
        "identityProviderId": ""
      }
    }


Get Specific User Information
*****************************

HTTP Request
============

- URL: ``/v2/users/{userId}``
- Method: ``GET``
- Content-Type: ``application/json``
- Content:


HTTP Response
=============

.. code-block:: json

    {
      "user": {
        "id": "CreatedUser1",
        "primaryParty": "",
        "isDeactivated": false,
        "metadata": {
          "resourceVersion": "0",
          "annotations": {}
        },
        "identityProviderId": ""
      }
    }

Delete Specific User
********************

HTTP Request
============

- URL: ``/v2/users/{userId}``
- Method: ``DELETE``
- Content-Type: ``application/json``
- Content:


HTTP Response
=============

.. code-block:: json

    {}

An empty response with a status 200 confirms successful deletion.


List Users
**********

HTTP Request
============

- URL: ``/v2/users``
- Method: ``GET``

HTTP Response
=============

.. code-block:: json

    {
      "users": [
        {
          "id": "CreatedUser",
          "primaryParty": "",
          "isDeactivated": false,
          "metadata": {
            "resourceVersion": "0",
            "annotations": {}
          },
          "identityProviderId": ""
        },
        {
          "id": "participant_admin",
          "primaryParty": "",
          "isDeactivated": false,
          "metadata": {
            "resourceVersion": "0",
            "annotations": {}
          },
          "identityProviderId": ""
        }
      ],
      "nextPageToken": ""
    }

Grant User Rights
*****************

HTTP Request
============

- URL: ``/v2/users/{userId}/rights``
- Method: ``POST``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "identityProviderId" : "",
      "rights" : [ {
        "kind" : {
          "ParticipantAdmin" : {
            "value" : { }
          }
        }
      } ],
      "userId" : "CreatedUser1"
    }

HTTP Response
=============

.. code-block:: json

    {
      "newlyGrantedRights": [
        {
          "kind": {
            "ParticipantAdmin": {
              "value": {}
            }
          }
        }
      ]
    }

Returns the rights that were newly granted.

Revoke User Rights
******************

HTTP Request
============

- URL: ``/v2/users/{userId}/rights``
- Method: ``PATCH``
- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {
      "identityProviderId" : "",
      "rights" : [ {
        "kind" : {
          "ParticipantAdmin" : {
            "value" : { }
          }
        }
      } ],
      "userId" : "CreatedUser1"
    }

HTTP Response
=============

.. code-block:: json

    {
      "newlyRevokedRights": [
        {
          "kind": {
            "ParticipantAdmin": {
              "value": {}
            }
          }
        }
      ]
    }

Returns the rights that were actually revoked.



List Specific User Rights
*************************

HTTP Request
============

- URL: ``/v2/users/{userId}/rights``
- Method: ``GET``
- Content-Type: ``application/json``

HTTP Response
=============

.. code-block:: json

    {
      "rights": [
        {
          "kind": {
            "CanActAs": {
              "value": {
                "party": "Bob::122099e6b7c92e12eb026f483f59a73e48149dc6e630a4f4f8fb95b8d269219b356c"
              }
            }
          }
        },
        {
          "kind": {
            "ParticipantAdmin": {
              "value": {}
            }
          }
        }
      ]
    }

List All DALF Packages
**********************

HTTP Request
============

- URL: ``/v2/packages``
- Method: ``GET``


HTTP Response
=============

.. code-block:: json

    {
      "packageIds": [
        "9e70a8b3510d617f8a136213f33d6a903a10ca0eeec76bb06ba55d1ed9680f69",
        "0e4a572ab1fb94744abb02243a6bbed6c78fc6e3c8d3f60c655f057692a62816",
        "5aee9b21b8e9a4c4975b5f4c4198e6e6e8469df49e2010820e792f393db870f4",
        "a1fa18133ae48cbb616c4c148e78e661666778c3087d099067c7fe1868cbb3a1",
        "a36cfff1da2f15da3d760c872860bf03140aa699f73b57b639fd40dfe8156cfe",
        "54f85ebfc7dfae18f7d70370015dcc6c6792f60135ab369c44ae52c6fc17c274"
        ]
     }

Where ``packageIds`` is the JSON array containing the package IDs of all loaded DALFs.

Download a DALF Package
***********************

HTTP Request
============

- URL: ``/v2/packages/<package ID>``
- Method: ``GET``
- Content: <EMPTY>

Note that the desired package ID is specified in the URL.

HTTP Response, status: 200 OK
=============================

- Transfer-Encoding: ``chunked``
- Content-Type: ``application/octet-stream``
- Content: <DALF bytes>

The content (body) of the HTTP response contains raw DALF package bytes, without any encoding. Note that the package ID specified in the URL is actually the SHA-256 hash of the downloaded DALF package and can be used to validate the integrity of the downloaded content.

Upload a DAR File
*****************

HTTP Request
============

- URL: ``/v2/packages``
- Method: ``POST``
- Content-Type: ``application/octet-stream``
- Content: <DAR bytes>

The content (body) of the HTTP request contains raw DAR file bytes, without any encoding.

HTTP Response, Status: 200 OK
=============================

- Content-Type: ``application/json``
- Content:

.. code-block:: json

    {}
