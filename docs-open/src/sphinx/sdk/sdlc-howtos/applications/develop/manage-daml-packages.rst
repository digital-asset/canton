..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _howto-applications-manage-daml-packages:

How to upload and query Daml packages
#####################################

Canton Participant Node exposes a package management service that allows uploading and discovery of the Daml packages.
This guide explains how to programmatically manipulate the packages using the JSON Ledger API, which is described using
:subsiteref:`OpenAPI specifications <reference-json-api-openapi>`.

To learn about the Daml packages, see :subsiteref:`Daml packages and archive (.dar) files <daml-packages-and-daml-archive-files>`.

Refer to the package management section in the operational guide to learn how to upload packages and inspected them
using :externalref:`the Canton console <manage-daml-packages-and-archives>`.

Prerequisites
=============

Ensure that your Canton Participant Node opens a JSON Ledger API HTTP port. To learn about how to do it, read the tutorial:
:ref:`Get started with Canton and the JSON Ledger API <tutorial-canton-and-the-json-ledger-api>`.

Ensure that you have access to Daml tools such as the Assistant (`daml`) and the Compiler (`damlc`).

Install `curl` or other similar tool that facilitates interactions over the HTTP protocol.

If you want to be able to format and filter JSON output from `curl` command responses, install `jq` or a similar tool.

Before any ledger interaction, ensure to build the model into a .dar file.

How to upload a DAR archive file
================================

Assume you are working on a Daml model called `MyModel`, and you want to start on-ledger interactions based on this model.
The first step is to upload its containing package and to make sure it is vetted on all the interacting Participant Nodes.

The JSON Ledger API provides endpoints that enable you to upload the package and get it vetted on the Participant Node
as an intrinsic part of the upload process.

As most of the interactions are based on a package ID, use the damlc compiler to inspect the resulting DAR archive file
and extract the package ID of your project's main package.

.. code-block:: sh

    dpm damlc inspect-dar --json .daml/dist/mymodel-1.0.0.dar | jq '.main_package_id'

The damlc responds with the package ID:

.. code-block:: none

    "47fc5f9bf30bdc147465d7b5fe170a0bc26b3677b45b005573130d951fdaebed"

To upload the package, invoke a POST command on the `v2/packages` endpoint:

.. code-block:: sh

    curl --data-binary @.daml/dist/mymodel-1.0.0.dar http://localhost:7575/v2/packages

You can verify that the package is present and registered on the ledger.

.. code-block:: sh

    curl -s http://localhost:7575/v2/packages/47fc5f9bf30bdc147465d7b5fe170a0bc26b3677b45b005573130d951fdaebed/status

The ledger responds with the package status:

.. code-block:: json

    {
      "packageStatus": "PACKAGE_STATUS_REGISTERED"
    }


How to query for existing packages
==================================

To list all packages known to the Participant Node, issue a GET request towards the `v2/packages` endpoint.

.. code-block:: sh

   curl -s http://localhost:7575/v2/packages

The Participant responds with a message containing all the known packages:

.. code-block:: json

    {
      "packageIds": [
        "9e70a8b3510d617f8a136213f33d6a903a10ca0eeec76bb06ba55d1ed9680f69",
        "47fc5f9bf30bdc147465d7b5fe170a0bc26b3677b45b005573130d951fdaebed",
        "bfda48f9aa2c89c895cde538ec4b4946c7085959e031ad61bde616b9849155d7"
      ]
    }

If the list is long, use `jq` to filter the output and determine if the expected package ID is among
the results.

.. code-block:: sh

    curl -s http://localhost:7575/v2/packages | jq '.packageIds | .[] | select(startswith("47"))'
