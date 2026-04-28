..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _tutorial-canton-and-the-json-ledger-api-ts-multisync:

=====================================================================
Multi-Synchronizer Operations with the JSON Ledger API and TypeScript
=====================================================================

This tutorial shows you how to work with a multi-synchronizer Canton setup using the JSON Ledger API and TypeScript.

Overview
========

You will explore an example project that demonstrates five key scenarios:

1. Allocating parties and packages across multiple synchronizers
2. Manually reassigning a contract from one synchronizer to another
3. Triggering automatic reassignment via a cross-synchronizer contract fetch
4. Observing a failed reassignment caused by a missing party on the target synchronizer
5. Observing a failed reassignment caused by an unvetted package on the target synchronizer

The example uses **external parties** authenticated with Ed25519 key pairs and submits all commands via the interactive submission API (prepare + execute).

Topology
--------

The Canton sandbox is started with the ``--multi-sync`` flag, which brings up the following topology:

- **Three participants**: ``sandbox`` (V1), ``pebblebox`` (V2), ``sidebox`` (V3)
- **Two synchronizers**: ``synchronizer-1`` (S1), ``synchronizer-2`` (S2)
- **Connectivity**: V1 connected to S1 and S2, V2 connected to S1 and S2, V3 connected to S2 only

Prerequisites
=============

Tools
-----

Before starting, ensure you have the following installed:

- **Node.js and npm** — Download from https://nodejs.org/en/download/. Recommended version: `18.20.x` or later.
- **Dpm** — Install following :externalref:`these instructions<dpm-install>`
- **Canton** — Includes pre-built examples. See the :externalref:`Canton demo<demo>` for details.

Example Project
---------------

Open a terminal and navigate to the multi-synchronizer example folder:

.. code-block::

    cd <canton_installation>/examples/14-multisync

Start Canton with the multi-sync flag:

.. code-block::

    ./run.sh

Wait until both synchronizers are visible on V1. Then open a new terminal and install the project dependencies:

.. code-block::

    npm install

Build the TypeScript code:

.. code-block::

    npm run build

Run all scenarios:

.. code-block::

    npm run scenario

You should see output for each of the five scenarios described below.

Scenario 1: Allocations
=======================

**File**: ``src/scenario1_allocations.ts``

This scenario sets up the full topology needed by all subsequent scenarios. It performs the following steps:

1. Discovers the connected synchronizers on V1 and identifies S1 and S2 by alias.
2. Verifies that V2 is connected to both synchronizers and that V3 is connected to S2 only.
3. Generates Ed25519 key pairs for three external parties: P1, P2, and P3.
4. Allocates P1 and P2 on both S1 and S2 (via V1 and V2 respectively) and allocates P3 on S2 only (via V3).
5. Uploads and vets the DAR on V1 and V2 for both synchronizers, and on V3 for S2 only.
6. Creates one user per participant, granting the appropriate ``CanActAs`` rights.

The topology returned by this scenario is a ``Topology`` object that is passed into all subsequent scenarios.

Scenario 2: Manual Reassignment
================================

**File**: ``src/scenario2_manual_reassignment.ts``

This scenario shows how to move a contract from one synchronizer to another using the explicit unassign/assign API.

Steps:

1. P1 and P2 co-sign the creation of an ``Iou`` contract on S1.
2. P1 unassigns the ``Iou`` from S1, targeting S2. This produces a ``reassignmentId``.
3. P1 assigns the ``Iou`` to S2 using the ``reassignmentId``.
4. P3 exercises the ``Iou_Inspect`` choice on the contract via explicit disclosure, using V2 as the submitting participant.

Scenario 3: Automatic Reassignment
=====================================

**File**: ``src/scenario3_automatic_reassignment.ts``

This scenario demonstrates that Canton can automatically reassign a contract when a transaction requires it on a different synchronizer.

Steps:

1. P1 and P2 create an ``Iou`` contract (C1) on S1.
2. P1 creates an ``IouLinked`` contract (C2) on S2 that holds a reference to C1.
3. P1 exercises the ``ProcessWithIou`` choice on C2 targeting S2. Because C1 is on S1, Canton automatically reassigns it to S2 before executing the choice.
4. P3 inspects C1 (now on S2) via explicit disclosure.

Scenario 4: Failed Reassignment (Missing Party)
================================================

**File**: ``src/scenario4_failed_missing_party.ts``

This scenario shows that reassignment is rejected when a signatory's participant is not connected to the target synchronizer.

Recall from Scenario 1 that P3's participant (V3) is only connected to S2. If a contract has P3 as a signatory, it cannot be reassigned to S1 because V3 has no connection there.

Steps:

1. P1 and P3 create an ``Iou`` contract on S2 (P3 is a signatory via the ``owner`` field).
2. P1 attempts to unassign the contract from S2 to S1.
3. The unassign call is rejected by Canton with a message indicating that P3 is not active on the target synchronizer.

The error message from Canton includes ``"not active on the target synchronizer"``.


Scenario 5: Failed Reassignment (Missing Vetting)
==================================================

**File**: ``src/scenario5_failed_missing_vetting.ts``

This scenario shows that reassignment is also rejected when the contract's package is not vetted on the target synchronizer, even if all parties are present there.

Steps:

1. The ``model-tests`` package is unvetted on S2 for both V1 and V2.
2. P1 and P2 create an ``Iou`` contract on S1 (the package is still vetted on S1).
3. P1 attempts to unassign the contract from S1 to S2.
4. The unassign call is rejected because the package is not vetted on S2.
5. The package is re-vetted on S2 (in a ``finally`` block, so the topology is restored regardless of the outcome).

The error message from Canton includes ``"has not vetted"``.

Vetting is a per-participant, per-synchronizer property. A package must be vetted on every participant connected to the target synchronizer for a contract using that package to be reassigned there.

Related
=======

- :ref:`tutorial-canton-and-the-json-ledger-api-ts` covers the basics of the JSON Ledger API with TypeScript using a single synchronizer.
- A :ref:`WebSocket variant <tutorial-canton-and-the-json-ledger-api-ts-websocket>` demonstrates how to build a browser-based client.

