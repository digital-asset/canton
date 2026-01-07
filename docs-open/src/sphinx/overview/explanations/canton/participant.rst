..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

#################
Participant nodes
#################

.. wip::
   A closer look into the participant node

   Diagram:

   * PN connects to multiple synchronizers (details to be fleshed out in a separate section later)

   * gRPC Ledger API access with isolation for multiple users, Admin API for operator access

   * Private contract store (PCS) includes historic data, ACS doesn't.

..
  The following diagram shows the main components of a participant.

  .. https://lucid.app/lucidchart/effe03fe-e952-49e2-abc7-ac7a4cc56211/edit?page=0_0&invitationId=inv_f3433f52-c2ec-4854-9b36-58942657f191#
  .. image:: ./images/overview/canton-participant-components.svg
     :align: center
     :width: 100%

  A ledger application uses the **Ledger API** (at the top of the diagram) to send **commands** to a participant and
  to receive the corresponding events.
  The **command submission service** receives a command, parses it, and performs some basic validation.
  Next, the command is submitted to **DAMLe (DAML engine)**, which translates it to a **transaction**;
  a **command** consists only of a **root node** whereas a **transaction** also recursively contains all **consequences** of all exercise actions.
  Then, the **synchronizer router** chooses a synchronizer that is suitable for executing the transaction.

  The **transaction processor** translates the transaction to a **confirmation request**;
  in particular, it computes the view decomposition, embeds the transaction into a Merkle tree, and
  creates different envelopes tailored to the different members validating the request.
  It uses the **sequencer client** to send the confirmation request to the mediator and all participants involved in the transaction.

  The **transaction processor** also uses the sequencer client to **receive confirmation requests** from the synchronizer,
  to **send mediator responses,** and to **receive the result messages** from the mediator.

  The **parallel indexer subscription** reads events from the **record order publisher** event log and stores them in a format that is optimized for **fast read access.**
  The **command completion service** allows ledger applications to read the completions corresponding to the commands it has previously submitted.
  The **transaction service** provides a stream of all transactions that have been committed to the virtual shared ledger and are visible to the participant.

.. _package-vetting-overview:
     
Package vetting
***************

.. todo:: <https://github.com/DACH-NY/canton/issues/25695>
  Package = 1 deployable unit of smart-contract code, does not include dependencies

  DAR = self-contained set of packages

  Announce knowledge of package (required for validation) and opt-in executing packages in the first place

  * safeguard against rogue packages

  * useful for selecting upgrades (cross-reference to build subsite?) and controlling synchronizer selection (forward ref to multi-synchronizer)


Concurrent / conflicting transactions
*************************************

.. todo:: <https://github.com/DACH-NY/canton/issues/25695>
   Contention: Conflict detection with locking

   Aggressive retries lead to lower success rates

   Back-reference from the relevant performance howto sections in Build / Operate

..
  .. _conflict-detection-overview:

  Conflict Detection
  ~~~~~~~~~~~~~~~~~~

  Participants detect conflicts between concurrent transactions by locking the contracts that a transaction consumes.
  The participant locks a contract when it validates the confirmation request of a transaction that archives the contract.
  The lock indicates that the contract might be archived.
  When the mediator's decision arrives later, the contract is either archived or unlocked,
  depending on whether the transaction is committed or rolled back.
  Transactions that attempt to use a locked (i.e., potentially archived) contract are rejected.
  This design decision is based on the optimistic assumption that transactions are typically accepted;
  the later conflicting transaction can therefore be pessimistically rejected.

  The next three diagrams illustrate locking and pessimistic rejections
  using the :brokenref:`counteroffer <counteroffer-acceptance>` example from the Daml ledger model.
  There are two transactions and three parties and every party runs their own participant node.
  
  * The painter `P` accepts `A`\ 's `Counteroffer` in transaction `tx1`.
    This transaction consumes two contracts:

    - The IOU between `A` and the `Bank`, referred to as `c1`.

    - The `Counteroffer` with stakeholders `A` and `P`, referred to as `c2`.

    The created contracts (the new Iou and the `PaintAgree`\ ment) are irrelevant for this example.

  * Suppose that the `Counteroffer` contains an additional consuming choice controlled by `A`, e.g., Alice can retract her `Counteroffer`.
    In transaction `tx2`, `A` exercises this choice to consume the `Counteroffer` `c2`.

  Since the messages from the sequencer synchronize all participants on the (virtual) global time,
  we may think of all participants performing the locking, unlocking, and archiving simultaneously.

  In the first diagram, the sequencer sequences `tx1` before `tx2`.
  Consequently, `A` and the `Bank` lock `c1` when they receive the confirmation request,
  and so do `A` and `P` for `c2`.
  So when `tx2` later arrives at `A` and `P`, the contract `c2` is locked.
  Thus, `A` and `P` respond with a rejection and the mediator follows suit.
  In contrast, all stakeholders approve `tx1`;
  when the mediator's approval arrives at the participants, each participant archives the appropriate contracts:
  `A` archives `c1` and `c2`, the `Bank` archives `c1`, and `P` archives `c2`.

  .. https://www.lucidchart.com/documents/edit/327e02c7-4f72-4135-9100-f3787389fd42
  .. figure:: ./images/overview/pessimistically-rejected-conflict1.svg
     :align: center
     :name: pessimistically-rejected-conflict1

  When two transactions conflict while they are in flight, the later transaction is always rejected.

  The second diagram shows the scenario where `A`\ 's retraction is sequenced before `P`\ 's acceptance of the `Counteroffer`.
  So `A` and `P` lock `c2` when they receive the confirmation request for `tx2` from the sequencer and later approve it.
  For `tx1`, `A` and `P` notice that `c2` is possibly archived and therefore reject `tx1`, whereas everything looks fine for the `Bank`.
  Consequently, the `Bank` and, for consistency, `A` lock `c1` until the mediator sends the rejection for `tx1`.

  .. https://www.lucidchart.com/documents/edit/b4e21ea2-bd67-438e-aedd-d54b79496a30
  .. figure:: ./images/overview/pessimistically-rejected-conflict2.svg
     :align: center

  Transaction `tx2` is now submitted before `tx1`.
  The consumed contract `c1` remains locked by the rejected transaction
  until the mediator sends the result message.

  .. note::
     In reality, participants approve each view individually rather than the transaction as a whole.
     So `A` sends two responses for `tx1`:
     An approval for `c1`\ 's archival and a rejection for `c2`\ 's archival.
     The diagrams omit this technicality.

  The third diagram shows how locking and pessimistic rejections can lead to unnecessary rejections.
  Now, the painter's acceptance of `tx1` is sequenced before Alice's retraction like in the :ref:`first diagram <pessimistically-rejected-conflict1>`,
  but the Iou between `A` and the `Bank` has already been archived earlier.
  The painter receives only the view for `c2`, since `P` is not a stakeholder of the Iou `c1`.
  Since everything looks fine, `P` locks `c2` when the confirmation request for `tx1` arrives.
  For consistency, `A` does the same, although `A` already knows that the transaction will fail because `c1` is archived.
  Hence, both `P` and `A` reject `tx2` because it tries to consume the locked contract `c2`.
  Later, when `tx1`\ 's rejection arrives, `c2` becomes active again, but the transaction `tx2` remains rejected.

  .. https://www.lucidchart.com/documents/edit/8f5ea302-9cb7-4ea2-a2f7-8f0e02fab63d
  .. figure:: ./images/overview/pessimistically-rejected-conflict3.svg
     :align: center

  Even if the earlier transaction `tx1` is rejected later,
  the later conflicting transaction `tx2` remains rejected and
  the contract remains locked until the result message.


Indexing
********

.. todo:: <https://github.com/DACH-NY/canton/issues/25695>
   Prepare state updates and current state for serving to users

   Record-order publishing

   LAPI user vs. Daml party

   Not indexed: Template fields of contracts, point to PQS in build subsite


Command Deduplication
*********************

.. todo:: <https://github.com/DACH-NY/canton/issues/25695>
   * What is it useful for?

   * Change ID for deduplication (user ID + actAs + command ID)

   * Completion events emitted only on submitting participant, submission IDs

   * Time bounds (measured locally on the participant)

   * Injective agreement completion <-> submission if submission IDs are unique.

   * Note: UTXOs already provide a basic form of deduplication for self-conflicting commands. Less general as not all commands are self-conflicting and cannot choose the identifier to deduplicate on.
