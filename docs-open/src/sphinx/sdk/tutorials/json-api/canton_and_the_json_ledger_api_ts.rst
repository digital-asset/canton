..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _tutorial-canton-and-the-json-ledger-api-ts:

============================================================
Get Started with Canton, the JSON Ledger API, and TypeScript
============================================================

This tutorial shows you how to interact with a Canton Ledger using the JSON Ledger API and TypeScript.

Overview
========

You will use and modify an existing example project provided with the Canton distribution.

Prerequisites
=============

You should be familiar with the basics of TypeScript and tools such as `npm` and `node.js`.

Tools
-----

Before starting, ensure you have the following installed:

- **Node.js and npm** — Download from https://nodejs.org/en/download/. Recommended version: `18.20.x` or later.
- **Dpm** — Install following :externalref:`these instructions<dpm-install>`

.. parsed-literal::

    curl -sSL https://get.digitalasset.com/install/install.sh | sh -s |release|

Verify the installation by running:

.. code-block::

    dpm version --active

You should see a version equal to or later than |version|.

- **Canton** — Includes pre-built examples. See the :externalref:`Canton demo<demo>` for details.

Example TypeScript Project
--------------------------

Open a terminal and navigate to the JSON Ledger API example folder:

.. code-block::

    cd <canton_installation>/examples/09-json-ledger-api

Start Canton:

.. code-block::

    ./run.sh

Once the Canton console is ready, open a new terminal and navigate to the TypeScript example folder:

.. code-block::

    cd <canton_installation>/examples/09-json-ledger-api/typescript

Running the Example
===================

Install the project dependencies:

.. code-block::

    npm install

You may see some warnings, which can be ignored for now.

The JSON Ledger API provides an OpenAPI specification. You can use this to generate TypeScript client classes (stubs).

To generate the TypeScript client:

.. code-block::

    npm run generate_api

Check the generated code in the `generated/api` folder. It should include the TypeScript classes for the API.

.. code-block::

    less generated/api/ledger-api.d.ts

This file contains definitions for services and models, such as `/v2/commands/submit-and-wait`, `CreateCommand`, and many others.

Next, generate TypeScript types from the Daml model:

.. code-block::

    npm run generate_daml_bindings

This creates bindings in the `./generated` folder. Each Daml module has its own subfolder, for example: `generated/model-tests-1.0.0/lib/Iou`.

Now compile the TypeScript code:

.. code-block::

    npm run build

Once the build succeeds, run the example:

.. code-block::

    npm run scenario

You should see output similar to:

.. code-block::

    Alice creates contract
    Ledger offset: 23
    ...
    Bob accepts transfer
    ...
    End of scenario

Code Highlights
===============

The JSON Ledger API client is configured in `src/client.ts`:

.. code-block:: typescript

    import createClient from "openapi-fetch";
    import type { paths } from "../generated/api/ledger-api";

    export const client = createClient<paths>({ baseUrl: "http://localhost:7575" });

The `openapi-fetch` library is used to create the API client.

Allocating a Party
------------------

In `src/user.ts`, a party is allocated as follows:

.. code-block:: typescript

    const resp = await client.POST("/v2/parties", {
        body: {
            partyIdHint: user,
            identityProviderId: "",
        }
    });

.. note::
    `openapi-fetch` uses the `Indexed Access Types <https://www.typescriptlang.org/docs/handbook/2/indexed-access-types.html>`_ TypeScript feature to provide type safety. The example above looks like untyped JavaScript, but it is in fact type-safe.

Creating a Contract
-------------------

In `src/commands.ts`, a contract is created using:

.. code-block:: typescript

    export async function createContract(userParty: string): Promise<components["schemas"]["SubmitAndWaitResponse"]> {
        const iou: Iou.Iou = {
            issuer: userParty,
            owner: userParty,
            currency: "USD",
            amount: "100",
            observers: []
        };

        const command: components["schemas"]["CreateCommand"] = {
            createArguments: iou,
            templateId: Iou.Iou.templateId
        };

        const jsCommands = makeCommands(userParty, [{ CreateCommand: command }]);
        const resp = await client.POST("/v2/commands/submit-and-wait", {
            body: jsCommands
        });

        return valueOrError(resp);
    }

Querying Contracts
------------------

In `src/index.ts`, contracts are queried like this:

.. code-block:: typescript

    const { data, error } = await client.POST("/v2/state/active-contracts", {
        body: filter
    });

    if (data === undefined)
        return Promise.reject(error);
    else {
        const contracts: components["schemas"]["CreatedEvent"][] = data
            .map((res) => res.contractEntry)
            .filter((res) => "JsActiveContract" in res)
            .map((res) => res.JsActiveContract.createdEvent);

        return Promise.resolve(contracts);
    }

Extending the Example
=====================

In this step, you extend the `Iou` template with a new field and update the TypeScript code accordingly.

1. Open the `Io.daml` file in `canton/examples/09-json-ledger-api/model`.

2. Modify the `Iou` template to include a new `comment` field:

.. code-block:: daml

    template Iou
      with
        issuer : Party
        owner : Party
        currency : Text
        amount : Decimal
        observers : [Party]
        comment : Optional Text -- added field
      where
        -- leave the rest of the template unchanged

> The `comment` field is optional, making this a backwards-compatible change.

3. Stop the Canton server (`Ctrl+C`) and restart it:

.. code-block::

    ./run.sh

4. Rebuild the TypeScript bindings:

.. code-block:: bash

    npm run generate_daml_bindings

5. Update the `createContract` function in `src/commands.ts` to include the new field:

.. code-block:: typescript

    export async function createContract(userParty: string): Promise<components["schemas"]["SubmitAndWaitResponse"]> {
        const iou: Iou.Iou = {
            issuer: userParty,
            owner: userParty,
            currency: "USD",
            amount: "100",
            observers: [],
            comment: "This is a test comment" // new field
        };
        // leave the rest of the function unchanged

6. Rebuild the TypeScript code:

.. code-block::

    npm run build

7. Run the example:

.. code-block::

    npm run scenario

You won’t see the new `comment` field in the output yet. To display it, modify the `showAcs` function call in `src/index.ts` to include the new field:

.. code-block:: typescript

    showAcs(contracts, ["owner", "amount", "currency", "comment"], c => [c.owner, c.amount, c.currency, c.comment]);

The, execute the `npm run scenario` once again.

Related
=======

A :ref:`TypeScript <tutorial-canton-and-the-json-ledger-api-ts-websocket>` version is also available that demonstrates how to create a JSON Ledger API client for use in a web browser.
