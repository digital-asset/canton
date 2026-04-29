..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _howto-multi-synchronizer-best-practices:

=============================================
Multi-Synchronizer Application Best Practices
=============================================

Overview
========

Multi-synchronizer applications improve scalability, tighten cost and access control, and may enable better regulatory oversight by distributing workflows across multiple Canton synchronizer instances. This architecture, however, introduces a unique set of challenges. Developers, application providers, application users, and infrastructure operators as well as interacting organizations must solve new problems and define additional operational processes.
It is critical that multi-synchronizer considerations are front and center throughout the entire project lifecycle: from inception and design through implementation, deployment, configuration, and day-to-day administration. This article will detail the best practices to manage these complexities.

Be ready for multi-synchronizer data
====================================

The Ledger API's Transaction and Active Contract Set (ACS) streams will now include new event types — ``Unassign`` and ``Assign`` — to indicate when contracts move between different synchronizers. At a participant, events such as ``Create``, ``Exercise``, ``Archive``, and these new assignment events will be received from multiple synchronizers and will be interspersed with each other. The synchronizer of origin is always clearly tagged on the corresponding messages. Make sure that your Operational Data Store (ODS) and your application logic is prepared to store and process this additional information.  Alternatively, use PQS in your application which already supports this.

Design reassignment steps
=========================

Design reassignment steps explicitly as part of workflow design.
In order to avoid confusion and hard to understand bugs the notion of the target synchronizer should be threaded through your application logic. Operations issued on the Ledger API can either be explicitly tagged with a target synchronizer or rely on an implicitly selected one. In the latter case the automatic selection should never come as a surprise to the application designers.
Automatic reassignment will only occur if all the following requirements are satisfied:

- The submitting party must be a stakeholder on every contract requiring reassignment.
- The executing participant node must be connected to the intended target synchronizer, as well as to all synchronizers currently hosting the input contracts.
- If the submission is externally signed, the submitting party must be hosted on the executing participant node.

Should any of these conditions not be met, automatic reassignment will fail, and the submission will be rejected.


Be resilient to out-of-order data
=================================

When a participant is disconnected from one of the synchronizers or is slow at processing messages from one of the sources, it may observe the events related to a given contract :externalref:`out of order <non-causality-of-updates-stream>`. For example, in rare circumstances an Archive event of a contract ID may appear before the Create event for the contract ID. Make sure that your client application is prepared for that possibility and correctly handles out of order events.

Design the vetting state
========================

Make sure you understand the implications of the asymmetries in the package (DAR) vetting states between the synchronizers. In fact you can use such asymmetries to your advantage. You may provide a small package that offers a very limited set of necessary templates and upload it to the global synchronizer and a large complex package with an elaborate set of interconnected templates on the private synchronizer. Such an approach simplifies the deployment, streamlines future upgrades by limiting the necessary vetting operations.
We can envision the application, which is based on the token standard, as having the following division.

- global-sync.dar
    - vetted on private and Global Synchronizer
    - contains:
        - Token contract implementing `Holding interface <https://docs.sync.global/app_dev/api/splice-api-token-holding-v1/Splice-Api-Token-HoldingV1.html>`_
        - TokenAllocation implementing  Holding and `Allocation interface <https://docs.sync.global/app_dev/api/splice-api-token-allocation-v1/Splice-Api-Token-AllocationV1.html>`_
- private-sync.dar
    - vetted on private synchronizer only
    - contains:
        - TokenRules contract implementing `TransferFactory <https://docs.sync.global/app_dev/api/splice-api-token-transfer-instruction-v1/Splice-Api-Token-TransferInstructionV1.html#interfaces>`_ and `AllocationFactory <https://docs.sync.global/app_dev/api/splice-api-token-allocation-instruction-v1/Splice-Api-Token-AllocationInstructionV1.html#interfaces>`_
        - templates required for minting, burning, and other administrative workflows

Design the party topology state
===============================

The deployment architecture must be considered at design time for both the party-to-participant mapping and the participant privilege levels across all connected synchronizers.
Configuration imbalances can sometimes be beneficial. For instance, a party might be hosted by multiple participants on one synchronizer for redundancy, but hosted by a single participant on a different synchronizer for cost-saving purposes. Applications must incorporate the implications of these asymmetric hosting choices into their logic.
Failure to do so can result in difficult-to-diagnose bugs. The following section outlines potential pitfalls to be aware of.

- A contract may fail to be moved due to missing :ref:`reassigning participants <def-multi-sync-reassigning-participant>` for some of the interacting parties. This may apply to signatories as well as observers.
- Unassign and assign operation pair may fail because of a missing :ref:`signatory unassigning <def-multi-sync-signatory-unassigning-participant>` or :ref:`signatory assigning participant<def-multi-sync-signatory-assigning-participant>` that could confirm it.
- A contract may leave or enter participant visibility. See description in this :ref:`article <entering-leaving-visibility>`

Avoid read contention problems
==============================

A read contention occurs when parties prior to exercising a transaction move contracts in opposite directions. Consider the scenario

- Participant P1 is connected to S1 and S2
- Contract C1 is assigned to S1
- Contract C2 is assigned to S2
- Party Alice reassign contract C2 to S1
- Party Bob reassign contract C1 to S2

At the application design time, consider the optimal workflow design with clear designation of synchronizers so that chances for read contention are minimized,

Plan synchronizer upgrades
==========================

Not all synchronizers will be able to interact with each other. In particular if the target synchronizer does not support the specific Daml-LF or hashing version used for a given contract, the reassignment will fail during the validation phase.
Therefore it is important to schedule the synchronizer upgrades in lock step with the Global Synchronizer.

Monitor participant connections
===============================

Participants spanning both synchronizers stay connected to avoid failed reassignments (if participant is needed for confirmation) and to avoid out-of-order assignment situations which can be confusing to users and 3rd party client software.

Avoid vetting gaps
==================

Missing vetting on one of the hosting participants may cause the contract reassignment to fail. Make sure the same package versions are vetted on all participants. You may fail to move a contract of a newer version if one of the hosting participants is only supporting earlier versions.
