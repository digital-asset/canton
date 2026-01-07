..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _synchronizer-overview:

Synchronizer
============

A Synchronizer is a foundational part of the Canton architecture that provides two main functionalities:

#. Sequencing: Ordered and confidential communication between independent participant nodes (validators).
#. Mediating: Transaction coordination as part of the two-phase commit protocol to provide atomicity and privacy.

Canton is designed as a network of networks where participant nodes may be connected to multiple Synchronizers at the same time.

Role in the Canton architecture
-------------------------------

* **Message Queue and Ordering:**
  The Synchronizer serves as a secure message queue, receiving encrypted transaction requests from participant nodes and ensuring they are ordered consistently.
  This ordering is critical for resolving potential conflicts and achieving consistent outcomes.

* **Privacy by Design:**
  Unlike traditional public blockchains where transaction data is public, Canton's Synchronizer **can not decrypt** the transaction payloads.
  Messages are always encrypted end-to-end between the relevant participants (validators).
  The Synchronizer only sees encrypted envelopes and their metadata for routing and ordering, maintaining privacy for sensitive transaction information.

* **Not a Validator:**
  It's important to note that the Synchronizer is **not responsible for validating the transaction contents.**
  That responsibility lies with the stakeholder participant nodes (validators) involved in a given transaction, using Canton's two-phase commit protocol.
  The Synchronizer merely ensures the consistent sequencing and delivery of these transaction requests and coordinates the outcomes of the transactions.

Technical components
--------------------

* **Sequencer:** The Sequencer is responsible for delivering the messages to the designated recipients adhering to the atomic multicast properties with privacy.

* **Mediator:**
  The Mediator works in conjunction with the Sequencer.
  It aggregates transaction confirmations from the involved stakeholder participant nodes (validators) and either approves or rejects a transaction based on the confirmations.
  It plays a key role in the two-phase commit protocol and provides privacy between the different participants involved in a transaction.

* **Ordering Layer:** The ordering layer establishes a consistent ordering of messages going through the Sequencer and assigns timestamps to the ordered messages, which is fundamental for conflict detection of transactions.

Types of Synchronizers
----------------------

* **Centralized Synchronizers:**
  While Canton is designed for decentralization, a private Canton deployment can use a centralized Synchronizer operated by a single entity.
  This is suitable for closed consortia or internal enterprise use cases.

* **Decentralized Synchronizers:**
  This is the more public and robust form.
  The Global Synchronizer is operated by a network of independent "Super Validators" who jointly run the Sequencers using Byzantine fault-tolerant (BFT) consensus and Mediators with BFT state machine replication.
  This offers high resiliency and security in the sense of not relying on a single trusted third party.
