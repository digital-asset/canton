..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _research-publications:

Research Publications
=====================

Daml, Canton, and their underlying theory are described in the following research publications:

* `Daml: A Smart Contract Language for Securely Automating Real-World Multi-Party Business Workflows <https://arxiv.org/abs/2303.03749>`_
  describes the theory underlying Daml's language primitives for smart contracts and how Daml is compiled.

  Alexander Bernauer, Sofia Faro, Rémy Hämmerle, Martin Huschenbett, Moritz Kiefer, Andreas Lochbihler, Jussi Mäki, Francesco Mazzoli, Simon Meier, Neil Mitchell, Ratko G. Veprek.
  *Daml: A Smart Contract Language for Securely Automating Real-World Multi-Party Business Workflows.*
  In: `arXiv:2303.03749 <https://arxiv.org/abs/2303.03749>`_, 2023.

  **Abstract:**
  Distributed ledger technologies, also known as blockchains for enterprises, promise to significantly reduce the high cost of automating multi-party business workflows. We argue that a programming language for writing such on-ledger logic should satisfy three desiderata:
  
  #. Provide concepts to capture the legal rules that govern real-world business workflows.
  #. Include simple means for specifying policies for access and authorization.
  #. Support the composition of simple workflows into complex ones, even when the simple workflows have already been deployed.
  
  We present the open-source smart contract language Daml based on Haskell with strict evaluation.
  Daml achieves these desiderata by offering novel primitives for representing, accessing, and modifying data on the ledger, which are mimicking the primitives of today's legal systems.
  Robust access and authorization policies are specified as part of these primitives, and Daml's built-in authorization rules enable delegation, which is key for workflow composability.
  These properties make Daml well-suited for orchestrating business workflows across multiple, otherwise heterogeneous parties.
  
  Daml contracts run (1) on centralized ledgers backed by a database,
  (2) on distributed deployments with Byzantine fault tolerant consensus, and
  (3) on top of conventional blockchains, as a second layer via an atomic commit protocol. 

* `A Structured Semantic Domain for Smart Contracts <https://www.canton.io/publications/csf2019-abstract.pdf>`_
  describes how Canton relates to `Daml <https://www.daml.com>`_ and the `ledger model <https://docs.daml.com/concepts/ledger-model/index.html>`_.
  
  Extended abstract presented at `Computer Security Foundations 2019 <https://web.stevens.edu/csf2019/index.html>`_.

* `Authenticated Data Structures As Functors in Isabelle/HOL <https://www.canton.io/publications/fmbc2020.pdf>`_
  formalizes Canton's Merkle tree data structures in the theorem prover Isabelle/HOL.

  - Andreas Lochbihler and Ognjen Maric.
    *Authenticated Data Structures As Functors in Isabelle/HOL.*
    In: Bruno Bernardo and Diego Marmsoler (eds.) `Formal Methods for Blockchain <https://fmbc.gitlab.io/2020/>`_ 2020.
    OASIcs vol. 84, 6:1-6:15, 2020.
  - `DOI <https://doi.org/10.4230/OASIcs.FMBC.2020.6>`_
  - `Preprint PDF <https://www.canton.io/publications/fmbc2020.pdf>`_
  - `Pre-reecorded talk <https://www.youtube.com/watch?v=A9Q4G_pCSj4>`_
  - `Live presentation (1:48 to 12:50) <https://www.youtube.com/watch?v=mTM5D6MeBRw>`_

  A `longer version <https://www.canton.io/publications/iw2020.pdf>`_ was presented at the `Isabelle Workshop 2020 <https://sketis.net/isabelle/isabelle-workshop-2020>`_ (`recording <https://www.youtube.com/watch?v=GvSnSL8eSEw>`_).
  The `Isabelle theories <https://www.isa-afp.org/entries/ADS_Functor.html>`_ are available in the Archive of Formal Proofs.
  
  **Abstract:**
  Merkle trees are ubiquitous in blockchains and other distributed ledger technologies (DLTs).
  They guarantee that the involved systems are referring to the same binary tree, even if each of them knows only the cryptographic hash of the root.
  Inclusion proofs allow knowledgeable systems to share subtrees with other systems and the latter can verify the subtrees’ authenticity.
  Often, blockchains and DLTs use data structures more complicated than binary trees;
  authenticated data structures generalize Merkle trees to such structures.

  We show how to formally define and reason about authenticated data structures, their inclusion proofs, and operations thereon as datatypes in Isabelle/HOL.
  The construction lives in the symbolic model, i.e., we assume that no hash collisions occur.
  Our approach is modular and allows us to construct complicated trees from reusable building blocks, which we call Merkle functors.
  Merkle functors include sums, products, and function spaces and are closed under composition and least fixpoints.
  As a practical application, we model the hierarchical transactions of Canton, a practical interoperability protocol for distributed ledgers, as authenticated data structures.
  This is a first step towards formalizing the Canton protocol and verifying its integrity and security guarantees.

* `A semantic domain for privacy-aware smart contracts and interoperable sharded ledgers <https://www.canton.io/publications/cpp2021-slides.pdf>`_

  `Lightning talk <https://popl21.sigplan.org/details/CPP-2021-certified-programs-and-proofs-lightning-talks/6/A-semantic-domain-for-privacy-aware-smart-contracts-and-interoperable-sharded-ledgers>`_ presented at `Certified Proofs and Programs 2021 <https://popl21.sigplan.org/home/CPP-2021>`_.

  **Abstract:**
  
  Daml is a Haskell-based smart contract programming language
  used to coordinate business workflows across trust boundaries.
  Daml’s semantics are defined over an abstract ledger,
  which provides a clear semantics for Daml’s authorization rules, double-spending protection, and privacy guarantees.
  In its simplest form, a ledger is represented as a list of commits, i.e., hierarchical transactions and their authorizers.
  This representation allows for easy reasoning about Daml smart contracts because the total order hides the intricacies of a distributed, Byzantine-fault tolerant system.
  It is also adequate for Daml running on a single blockchain, as it defines a total order on all transactions.

  Yet, for distributed ledgers to fully eliminate data silos, smart contracts must not be tied to a single blockchain,
  which would then just become another silo.
  Daml therefore runs on different blockchains such as Hyperledger Fabric, Ethereum, and FISCO-BCOS as well as off-the-shelf databases.
  The underlying protocol Canton supports atomic transactions across all these Daml ledgers.
  This makes Daml ledgers sharded for higher throughput as well as interoperable to avoid data silos.

  Semantically, Canton creates a virtual shared ledger by merging the individual ledgers’ lists of commits.
  The virtual shared ledger is not totally ordered, to account for the fact that there is no global notion of time across ledgers.
  Still, transactions can use only contracts that have been created within earlier transactions.
  This ensures that causality is respected even though individual system users cannot see all dependencies due to the privacy rules.
  Canton tracks privacy-aware causality using vector clocks.

  To ensure that Daml and Canton achieve their claimed properties, we have started to formalize the Daml ledger model
  and prove its properties in Isabelle/HOL. The two main verification goals are as follows:

  #. Canton’s vector clock tracking correctly implements causality.

  #. The synchronization due to vector clocks cannot cause deadlocks.

  The challenge here is that these guarantees should hold for honest nodes in the system even if other systems fail or behave Byzantine.

  In the lightning talk, we give an idea of the ledger model, privacy-aware causality, and the current state of the verification.
