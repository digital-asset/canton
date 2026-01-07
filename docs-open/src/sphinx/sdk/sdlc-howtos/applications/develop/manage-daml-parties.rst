..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _howto-applications-manage-daml-parties:

How to allocate and query Daml parties
######################################

Canton Participant Node exposes a party management service that allows creation and discovery of the Daml parties. This guide
explains how to programmatically manipulate the parties using the JSON Ledger API, which is described using :ref:`OpenAPI specifications<reference-json-api-openapi>`.

To learn about the Daml parties and users, see :brokenref:'Daml parties and users' in the key
concepts section.

Refer to the party management section in the operational guide to learn how the parties can be created using
:externalref:`the Canton console<getting-started-with-parties>`.

Start Canton Participant Node
=============================

Ensure that your Canton Participant Node opens a JSON Ledger API HTTP port by adding a flag to the Canton
Participant startup

.. code-block:: sh

    -C canton.participants.participant1.http-ledger-api.port=7575

Alternatively, enable the JSON Ledger API in the Canton config file. The Canton community installation contains an example
under examples/09-json-api.

Start the Canton Participant Node and connect it to the Synchronizer. If you are unfamiliar with the procedure, follow
the steps described in :externalref:'the getting started tutorial <connecting-the-nodes>'.

How to query for existing parties
=================================

To list all parties known to the participant, issue a GET request towards the `v2/parties` endpoint.

.. code-block:: sh

   curl http://localhost:7575/v2/parties

The participant responds with a message containing all the known parties

.. code-block:: json

    {
      "partyDetails": [
        {
          "party": "Alice::122091f5d8d174bc0d624616d4f366904f8d4c56d56e33508878db3156c3dd9b8ae9",
          "isLocal": true,
          "localMetadata": {
            "resourceVersion": "0",
            "annotations": {}
          },
          "identityProviderId": ""
        },
        {
          "party": "Bob::122091f5d8d174bc0d624616d4f366904f8d4c56d56e33508878db3156c3dd9b8ae9",
          "isLocal": true,
          "localMetadata": {
            "resourceVersion": "0",
            "annotations": {}
          },
          "identityProviderId": ""
        },
        {
          "party": "ee1d49e9-fa52-480a-8e85-033738a1fc75::122091f5d8d174bc0d624616d4f366904f8d4c56d56e33508878db3156c3dd9b8ae9",
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

The `isLocal` attribute is set to true if the participant hosts the party and the party shares the same
identity provider as the user issuing the request.

How to create a new local party
===============================

To create a new party, issue a POST request towards the `v2/parties` endpoint.

.. code-block:: sh

   curl -d '{"partyIdHint":"Alice"}' http://localhost:7575/v2/parties

The resulting party id is composed of the supplied `partyIdHint` and the namespace fingerprint of the entity
overseeing that party. Typically, it is the fingerprint associated with the Canton Participant Node.

.. code-block:: json

    {
      "partyDetails": {
        "party": "Alice::122091f5d8d174bc0d624616d4f366904f8d4c56d56e33508878db3156c3dd9b8ae9",
        "isLocal": true,
        "localMetadata": {
          "resourceVersion": "0",
          "annotations": {}
        },
        "identityProviderId": ""
      }
    }

If you omit the `partIdHint` in your request, the Participant Node selects a random hint string.

