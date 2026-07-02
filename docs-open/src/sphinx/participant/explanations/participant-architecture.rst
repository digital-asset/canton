..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _participant-architecture:

Participant Architecture
========================

A Participant Node hosts Daml parties and allows them to synchronize state changes
with other participants through one or more :ref:`synchronizers <synchronizer-architecture>`.
This page describes the main components of a participant and how they interact.

Overview
--------

A participant node consists of the following high-level components:

* **Ledger API** -- the gRPC interface through which applications submit commands and
  subscribe to events. Access is scoped per user, and each user is associated with
  one or more Daml parties.

* **Admin API** -- the operator-facing interface for node management tasks such as
  party and package management, synchronizer connections, and pruning.

* **Daml Engine** -- evaluates Daml commands into full transactions by interpreting
  the smart-contract logic. A submitted command contains only a root action; the engine
  expands it into a complete transaction tree with all consequences.

* **Synchronizer Router** -- selects which synchronizer to use for executing a given
  transaction, based on where the input contracts are currently assigned.

* **Transaction Processor** -- the core protocol component. It translates a transaction
  into a confirmation request (computing the view decomposition and Merkle tree),
  sends the request through the sequencer client, collects confirmation responses from
  validating participants, and processes result messages from the mediator.

* **Sequencer Client** -- maintains the authenticated connection to one or more
  :ref:`sequencer nodes <protocol-sequencer-nodes>` on each connected synchronizer.
  All communication with other participants and with the mediator flows through this
  client.

* **Contract Store** -- persists the contracts that are relevant to the hosted parties.
  The *active contract set* (ACS) contains only currently active contracts, while the
  full *private contract store* (PCS) also retains archived contracts for
  historical queries.

* **Indexer** -- reads committed events from the record-order publisher and stores them
  in a format optimized for fast Ledger API reads. It feeds the transaction service,
  the command completion service, and the active contract service.

Command Processing Flow
-----------------------

When an application submits a command through the Ledger API, the participant processes
it through the following stages:

1. The **command submission service** authenticates and validates the command.
2. The **Daml Engine** interprets the command to produce a full transaction.
3. The **synchronizer router** identifies the appropriate synchronizer.
4. The **transaction processor** prepares a confirmation request (view decomposition,
   encryption, Merkle tree) and sends it via the **sequencer client**.
5. Validating participants receive the request from the sequencer, validate the
   transaction views they can see, and send their confirmation responses to the
   **mediator** (again through the sequencer).
6. The **mediator** collects responses and publishes a result message (approve or reject).
7. On approval, each participant updates its contract store and the **indexer** makes
   the committed transaction available through the Ledger API.

Multi-Synchronizer Support
--------------------------

A participant can be connected to multiple synchronizers simultaneously. Each active
contract is *assigned* to exactly one synchronizer at any point in time. If a transaction
requires contracts that are assigned to different synchronizers, those contracts must
first be *reassigned* to a common synchronizer using the
:ref:`reassignment protocol <reassignment-protocol>`.

For details on multi-synchronizer operation, see :ref:`multiple-synchronizers`.

High Availability
-----------------

Participant nodes can be deployed in a high-availability (HA) configuration with active
and passive replicas sharing a common database. For details on replication and failover,
see :ref:`participant-ha`.

