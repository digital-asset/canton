..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

The Canton Network
==================

.. wip::
   Look inside the infrastructure that runs the multi-party application

   .. note::
      CREATE A 3-5min VIDEO / ANIMATED RECORDING


A Network of Networks
*********************

.. todo:: <https://github.com/DACH-NY/canton/issues/25652>
   (With diagram)

   * utilities as services provided by operators (reference to utilities documentation?)

   * participant nodes connected to multiple synchronizers

   * users interact with apps (incl. DA utilities) and possibly multiple participant nodes

   * global synchronizer at the center, private synchronizers around (almost star-shaped topology)

   * Canton Network ( = lots of independent Cantons with their own rules, including costs of transactions, operating under the joint law, see Finadium report/canton.network website for text)
     with global synchronizer as one common exchange

   .. note::
      Components can be operated in a centralized way (= operated by a single entity) or in a decentralized way.
      Decentralized means that from the clients' perspective (synchronizer's client = participant nodes, participant node's client = LAPI user) the infrastructure can be replicated in a Byzantine-fault tolerant way (forward ref to decentralization)


The Global Synchronizer
***********************

.. todo:: <https://github.com/DACH-NY/canton/issues/25652>
   What makes it special?

   * Trustworthy: The GSF (group of entities) who govern it and the decentralized operators of the nodes (not the same as the GSF though)

   * The ecosystem around it (CantonCoin, tokenomics, apps; cross-link to suitable sites)

   * Many apps are connected: the GS serves as the common hub for composed transactions spanning multiple apps (e.g. DvP settlement)

   * Not the technical level: it is just another synchronizer implementation-wise


Key differentiators
*******************

.. todo:: <https://github.com/DACH-NY/canton/issues/25652>
   * UTXO-style ledger with built-in sub-transaction privacy (projection to participant nodes and parties; forward ref to ledger model?)

   * Built-in delegation and authorization

   * App composition / interop with atomic transactions across apps / orgs

   Example: standards for composition (example CN token standard https://github.com/global-synchronizer-foundation/cips/blob/main/cip-0056/cip-0056.md)

   Add forward references to example use cases for each differentiator that absolutely needs it.



.. todo:: Parking lot <https://github.com/DACH-NY/canton/issues/25652>


   Canton is a Daml ledger interoperability protocol.
   Parties that are hosted on different participant nodes can transact using smart contracts written in Daml and the Canton
   protocol. The Canton protocol allows you to connect different Daml ledgers into a single virtual global ledger.
   Daml, as the smart contract language, defines who is entitled to see and who is authorized to change any given
   contract. The Canton synchronization protocol enforces these visibility and authorization rules and ensures that
   the data is shared reliably with very high levels of privacy, even in the presence of malicious actors. The Canton network can be extended without friction with new parties, ledgers, and applications building on other
   applications. Extensions require neither a central managing entity nor consensus within the global network.

   Canton faithfully implements the authorization and privacy requirements set out by Daml for its transactions.
   
   .. https://app.lucidchart.com/documents/edit/da3c4533-a787-4669-b1e9-2446996072dc/0_0
   .. figure:: ./images/topology.png
      :align: center
      :width: 80%

   Parties are hosted on participant nodes. Applications connect as parties to their Participant Node using the Ledger API.
   The participant node runs the Daml interpreter for the locally installed Daml smart contract code and stores the smart contracts
   in the *private contract store (PCS)*. The participants connect to synchronizers and synchronize their states
   with other participants by exchanging Canton protocol messages with other participants leveraging the synchronizer
   services. The use of the Canton protocol creates a virtual global ledger.

   Canton is written in Scala and runs as a Java process against a database (currently H2 and Postgres).
