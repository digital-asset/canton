..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _participant-overview:

Participant
===========

Participants form the core part of Canton's distributed ledger architecture.
A Participant provides a party-specific view of the distributed ledger, processes its Daml logic, and participates in the Canton protocol with other participants via the synchronizer.
In essence, a Canton Participant is the **private, self-sovereign computational and storage unit** for an entity within the Canton Network.

Core Technical Responsibilities
-------------------------------

.. todo:: Link to protocol description when referring to the canton protocol below <https://github.com/DACH-NY/canton/issues/25689>

#. **Daml Smart Contract Execution**

   * The Participant node is the **runtime environment for Daml smart contracts**.
     When a transaction involving a party hosted by this Participant is submitted or executed, the node performs the necessary Daml ledger operations (such as, creating, archiving, or exercising choices on contracts) using the Daml Engine.

   * It uses Daml's built-in authorization and privacy model to ensure that only the correct parties can initiate or observe ledger changes.

#. **Private Contract Store**

   * Unlike traditional blockchain nodes, which store a full copy of a global ledger, a Canton Participant maintains a **highly localized and private view of the ledger state**.
     It only stores the data for Daml contracts whose signatories and observers are hosted on the Participant.

   * This is fundamental to Canton's privacy guarantees, preventing data leakage to unauthorized parties.

#. **Transaction Validation**

   * When a transaction involves multiple parties, the respective Participant nodes for *each involved stakeholder* collectively validate the transaction.

   * Each Participant node verifies that the transaction respects Daml's authorization rules, that all consumed inputs are valid and unspent according to its local view (to prevent double spends), and that the resulting outputs are correctly derived.
     This distributed validation is part of Canton's two-phase commit protocol and does not rely on a central entity to validate transactions.

   * Sub-transaction privacy: Participants see only those parts of a transaction they are entitled to according to the privacy model.
     For the other parts of a transaction they are *not* entitled to, the Participants see neither any transaction payload nor metadata like involved Participants or parties.

#. **Synchronizer Connection**

   * A Participant connects to a Canton Synchronizer such as the Global Synchronizer or a private instance.

   * It uses the Synchronizer to exchange messages related to transaction proposals and confirmations with other Participants.

   * The Synchronizer provides the total ordering for these messages and facilitates the **two-phase commit protocol** that provides a consistent outcome across all involved participants.

   * A Participant may be connected to multiple Synchronizers and can orchestrate multi-Synchronizer transactions by reassigning contracts between different Synchronizers.

#. **Canton Ledger API Exposure:**

   * The Participant node exposes the **Ledger API** (a gRPC or JSON-based API) to client applications and users.
   * This API allows users to submit Daml commands (to create contracts or exercise choices on contracts), query contract states, and subscribe to ledger updates for their relevant parties.
