..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _external_signing_overview:

================
External Signing
================

What you'll learn
=================

In this set of tutorials, you will learn how to:

- :ref:`Use the Ledger API to onboard a party using an external key to sign transactions <tutorial_onboard_external_party_lapi>`
- :ref:`Use the Ledger API to onboard a multi-hosted external party <tutorial_onboard_external_multi_hosted>`
- :ref:`Create a contract using external signing <tutorial_externally_signed_transactions>`
- :ref:`Exercise a choice on a contract using external signing <tutorial_externally_signed_transactions_part_2>`

For more advanced use cases, the following tutorials will guide you through the process of using the Admin API to
manage parties or upload much more generic topology transactions using external signing:

- :ref:`Use the Admin API to onboard an external party <tutorial_onboard_external_party>`
- :ref:`Build, sign and submit topology transactions <externally_signed_topology_transaction>`

Context
=======

The ledger state in Canton is defined by **contracts owned by parties**. Each contract outlines the rights of different
parties to unilaterally change the shared ledger. From a party's perspective, there are two key activities:
**initiating transactions** and **validating transactions**.

A **party** is a logical actor on the ledger. While a party is an abstract concept, its on-ledger representation and
state management are delegated to one or more **validators** of its choice. Because of Canton's privacy properties, only
these validators are aware of the full set of contracts owned by a party. This means only they can authoritatively
validate and confirm transactions that affect the party's contracts.

Transactions may address only actively hosted parties. Transactions referring to invalid parties will be rejected by
the system.

Topology and Identities
-----------------------

Identities of parties, validators and synchronizers in Canton are expressed as **unique identifiers**, where each
identifier is a pair of a name and a fingerprint of a public key ``<prefix>::<fingerprint>``, as explained in the :ref:`topology management overview <topology-unique-identifiers>`. The public key is used to ultimately verify authorizations with respect to identities, as explained below.

How a party is exactly set up is defined by the set of topology transactions. These transactions need to be signed by
all affected parties and validators and submitted to the ledger. The sum of all topology transactions defines the
topology state, which is the shared state of knowledge about parties, keys, validators and packages on the ledger.

The topology state can evolve. A party is not tied to its initial configuration. However, replicating parties across
nodes is an :ref:`operational procedure between validator operators <party-replication>`, not covered in this section.

.. _external_parties_hosting_relationships:

Hosting Relationships
---------------------

To set up a party, a user must establish a **hosting relationship** with one or more validators. This hosting relationship
is expressed through a topology transaction named **party to participant mapping** which need to be signed by the party
and the validators involved. As part of this hosting relationship, the party defines which private key it will use to
authorize transactions affecting its contracts. This private key of the party can be managed by the user, or one can
delegate signing to the validator. This allows to distinguish two types of hosting setups, depending on how the
authorizing key of the party is managed:

- **External parties**, where the authorizing private key is owned and operated by the user of the party, which may be an
  end user wallet or a backend application.
- **Internal parties**, where the validators key is used to authorize transactions on behalf of the party, while the user
  authenticates itself using a JWT token on the Ledger API.

Internal parties are easier to use from an operational perspective, as they don't require safe-keeping of an additional
private key, but ultimately, they entrust the validators to not misuse the private key to authorize transactions.
The hosting relationship between a party and a participant is defined using three different categories of validator
permissions:

- **Submission**, where the validators signing key is used to authorize transactions
- **Confirmation**, where the validators signing key is only used to confirm transactions.
- **Observation**, where the validator is only informed and validates the state, but is not required to confirm.

Granting a validator **Submission** rights means that the party is an internal party. If the validators are
granted **Confirmation** or **Observation** rights, then the party is an external party for which the authorizing key
needs to be defined with an additional topology transaction, the **party to key mapping**.

Please continue with the following tutorial to learn how to
:ref:`onboard an external party using the Ledger API <tutorial_onboard_external_party_lapi>`.

Internal parties are covered in the :ref:`party management documentation <party-management>`.

Multi-Hosted Parties
--------------------

To reduce the need to fully trust any single validator, a party can configure its hosting relationship to require a
specific number of validators to approve a transaction before it is considered valid. This defines another dimension of
deployments:

- **Single hosted parties**, where a party is hosted by only a single validator. This is the simplest setup and is
  suitable for scenarios where the party owner fully trusts the validator.
- **Multi hosted parties**, where a party is hosted by multiple validators. This setup enhances security and availability
  by distributing trust across several validators.

Please follow the following tutorial to learn how to :ref:`setup a multi-hosted party using the Ledger API <tutorial_onboard_external_multi_hosted>`

To summarize, in order for a party to be configured to use the ledger, the following topology state needs to be defined:

- The name of the party and the signing key the party will use to authorize transactions.
- Which set of validators will host the party with which permissions and threshold.
- If necessary, any namespace delegations required to verify the signatures on the topology transactions.

Note that the hosting choices have a significant impact on the security and availability of the party. Please refer to
the trust assumptions outlined in the :ref:`party trust model <party_trust_model>`.

.. _external_parties_admin_api:

Managing Topology via Admin API
-------------------------------

The topology system is very flexible and powerful, but also complex. For topology transactions that require multiple
independent actors to sign, the process of building, signing and submitting these transactions can be tedious.
However, topology transactions can also be distributed as proposals through the ledger itself. A proposal is a
topology transaction that has not yet been signed by all the necessary actors.

How to use the Admin API for various topology management tasks related to parties is covered in the following tutorials:

- :ref:`Use the Admin API to onboard an external party <tutorial_onboard_external_multi_hosted>`
- :ref:`Build, sign and submit topology transactions <externally_signed_topology_transaction>`

Transaction Submission
----------------------
Once a party is set up, it can start using the ledger by submitting transactions that create contracts or exercise choices.
For parties that authorize using an external key, the transaction submission process involves the following steps:

- Submit a command to a validator node of choice to interpret the command and produce the resulting transaction. The
  transaction is not sent to the ledger yet, but returned to the user.
- Validate and sign the transaction hash which is a commitment to the entire resulting transaction (command and result) with the party's private key.
- Submit the pre-computed transaction and the signature to a validator node for submission to the ledger.
- Observe the result of the transaction through the validator nodes hosting the party.

Please refer to the :ref:`external signing overview <sdk_external_signing_overview>` for a more detailed explanation.

Note that the signature is a commitment to the entire transaction output, not just the command. If the interpreter was
faulty, then the validators will reject the transaction. Submission can happen through any node, not just one that
hosts the party.

How to submit commands using an external signing key is covered in the following tutorials:
- :ref:`Create a contract using external signing <tutorial_onboard_external_multi_hosted>`
- :ref:`Exercise a choice on a contract using external signing <tutorial_onboard_external_multi_hosted>`
