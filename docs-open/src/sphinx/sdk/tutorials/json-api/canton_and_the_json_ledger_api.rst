..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _tutorial-canton-and-the-json-ledger-api:

===============================================
Get started with Canton and the JSON Ledger API
===============================================

This tutorial demonstrates how to interact with the Canton Ledger using the JSON Ledger API from the command line. It uses `curl` and optionally `websocat` to communicate with the Ledger.

Overview
========

This example shows how to:

- enable the JSON Ledger API in the Canton configuration
- create a contract using the JSON Ledger API and basic Bash tools (`curl`)
- list active contracts using `curl`
- basic error handling and troubleshooting


Prerequisites
=============

Tools
-----

Before running the project, ensure you have the following installed:

- A Bash-compatible terminal (e.g., macOS Terminal, Git Bash, etc.)
- **Dpm** — Install following :externalref:`these instructions<dpm-install>`

.. parsed-literal::

    curl -sSL https://get.digitalasset.com/install/install.sh | sh -s |release|

    (check installed dpm version by entering in bash console :code:`dpm version --active`, it should be at least |version|)

- `canton` — Canton release with installation and examples, check :externalref:`Canton demo<demo>` for details
- `curl` — command-line HTTP client: https://github.com/curl/curl (installation: https://curl.se/download.html)
- (Optional) `jq` — command-line JSON processor: https://github.com/jqlang/jq (installation: https://jqlang.org/download/)


Daml Model
----------

To demonstrate the JSON Ledger API, you need an example Daml model.

Run the following command in the console:

.. code-block::

    dpm new json-tests

A folder named ``json-tests`` should be created. Check its contents and inspect the code in ``daml/Main.daml``.

It should contain a Daml model with a template named ``Asset``:

.. code-block::

    template Asset
      with
        issuer : Party
        owner  : Party
        name   : Text
      where
        ensure name /= ""
        signatory issuer
        observer owner
        choice Give : AssetId
          with
            newOwner : Party
          controller owner
          do create this with
            owner = newOwner

Before proceeding, compile the Daml model by running the following command inside the folder:

.. code-block::

    dpm build

A file named `.daml/dist/json-tests-0.0.1.dar` should be created.


Starting Canton with JSON Ledger API
====================================

First, ensure you can run the Canton sandbox with the JSON Ledger API enabled.

Navigate to the folder where you unpacked the canton distribution. There should be a `bin` subfolder containing the `canton` executable.

.. code-block::

    cd <canton_installation>



Start the Canton sandbox, providing a path to the created ``dar`` file:

.. code-block::

    ./bin/canton sandbox --json-api-port 7575 --dar <path_to_json_tests_project>/.daml/dist/json-tests-0.0.1.dar

.. note:: To simplify this tutorial, no security-related configuration is used. You will be able to send commands as any user.

Open a new Bash terminal, but keep the Canton sandbox running.


Verification - download OpenAPI
-------------------------------

To verify that the JSON Ledger API is working, check if the documentation endpoint is available.

In a new Bash terminal, run:

.. code-block::

    curl localhost:7575/docs/openapi

You should receive a long YAML document starting with:

.. code-block::

    openapi: 3.0.3
    info:
      title: JSON Ledger API HTTP endpoints
      version: 3.3.0
    paths:
      /v2/commands/submit-and-wait:
        post:

This `openapi.yaml` document provides an overview of the available endpoints and can be pasted into a tool like https://editor-next.swagger.io.

It can also be used to generate client stubs in languages like `Java` or `TypeScript`.

.. note:: You can use the `http://localhost:757/livez` endpoint to check whether the server is running. It might be used as a health check in production environments.


Create a party
--------------

Run this command in the terminal:

.. code-block::

    curl -d '{"partyIdHint":"Alice", "identityProviderId": ""}' -H "Content-Type: application/json" -X POST localhost:7575/v2/parties

This should return a response similar to:

.. code-block::

    {"partyDetails":{"party":"Alice::122084768362d0ce21f1ffec870e55e365a292cdf8f54c5c38ad7775b9bdd462e141","isLocal":true,"localMetadata":{"resourceVersion":"0","annotations":{}},"identityProviderId":""}}

Take note of the party ID, which will be used in the following steps. In this example, it is:

``Alice::122084768362d0ce21f1ffec870e55e365a292cdf8f54c5c38ad7775b9bdd462e141`` — but this will differ in your case.


Create a contract
=================

Create a file called `create.json` with the following content, replacing `<partyId>` with the actual party ID shown earlier:

.. code-block::

    {
      "commands": [
        {
          "CreateCommand": {
            "createArguments": {
              "issuer": "<partyId>",
              "owner": "<partyId>",
              "name": "Example Asset Name"
            },
            "templateId": "#json-tests:Main:Asset"
          }
        }
      ],
      "userId": "ledger-api-user",
      "commandId": "example-app-create-1234",
      "actAs": [
        "<partyId>"
      ],
      "readAs": [
        "<partyId>"
      ]
    }

Now submit the request:

.. code-block::

    curl localhost:7575/v2/commands/submit-and-wait -H "Content-Type: application/json" -d@create.json

A successful response should look like:

.. code-block::

    {"updateId":"...","completionOffset":20}

This confirms that a contract was created on the ledger.


Query the ledger
================

To query the ledger for active contracts, first create a file named `acs.json`:

.. code-block::

    {
      "eventFormat": {
        "filtersByParty": {},
        "filtersForAnyParty": {
          "cumulative": [
            {
              "identifierFilter": {
                "WildcardFilter": {
                  "value": {
                    "includeCreatedEventBlob": true
                  }
                }
              }
            }
          ]
        },
        "verbose": false
      },
      "verbose": false,
      "activeAtOffset": <offset>
    }

Replace `<offset>` with the `completionOffset` you received earlier (e.g., `20`).

Run the query:

.. code-block::

    curl localhost:7575/v2/state/active-contracts -H "Content-Type: application/json" -d@acs.json

You should receive a response containing contract information.

To improve readability, pipe the output to `jq`:

.. code-block::

    curl localhost:7575/v2/state/active-contracts -H "Content-Type: application/json" -d@acs.json | jq

Look for the `createdEvent` section, which contains contract details like:

.. code-block::

    "createdEvent": {
          "offset": 20,
          "nodeId": 0,
          "contractId": "00572c50513ced94f9cddaf1e6d2d3f050ae35d7fea0affe06f65f4238e84136edca1112202d01e45b5cfafb61e3942e4610547689dbebfebd3ea7d10d57944401fc17e81b",
          "templateId": "6fd1d46124d5ab0c958ce35e9bb370bb2835b2672a0d6fa039a3855c11b8801d:Main:Asset",
          "contractKey": null,
          "createArgument": {
            "issuer": "Alice::122084768362d0ce21f1ffec870e55e365a292cdf8f54c5c38ad7775b9bdd462e141",
            "owner": "Alice::122084768362d0ce21f1ffec870e55e365a292cdf8f54c5c38ad7775b9bdd462e141",
            "name": "Example Asset Name"
          },
      ...
    }

Troubleshooting
===============

If you encounter issues while calling the using curl - you should enable `-v` (verbose) mode to see the request and response details.
For instance:
.. code-block::

        curl -v -d '{"partyIdHint":"Alice", "identityProviderId": ""}' -H "Content-Type: application/json" -X POST localhost:7575/v2/parties

Http response different than 200 (e.g., 400, 404, etc.) indicates an error. The response body will contain details about the error.

If it does not help, read logs available in the canton sandbox terminal or in the file `<canton_installation>/logs/canton.log`.


If nothing is returned when you query `localhost:7575/v2/state/active-contracts` ensure that the offset provided is correct and corresponds to the `completionOffset` from the `localhost:7575/v2/commands/submit-and-wait` command.
You can also check current offset by running:
.. code-block::

    curl localhost:7575/v2/state/ledger-end


Next steps
==========

Canton examples
---------------

For a more advanced scenario involving two parties, explore the examples provided with Canton installation:
`<canton_installation>/examples/json-ledger-api>`

A `TypeScript` version is also available that demonstrates how to create a JSON Ledger API client for use in a web browser.

OpenAPI and AsyncAPI
--------------------

Read more about the OpenAPI and AsyncAPI specifications for the Canton JSON Ledger API: :ref:`references_json-api`.

Authentication and security
---------------------------

Read how to configure and use jwt token to access JSON Ledger API :ref:`json-api-access-tokens`.


Error codes
-----------

For more information about error codes returned by the JSON Ledger API, see :ref:`json-error-codes`.

