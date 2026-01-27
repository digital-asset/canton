..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _multi-sig-in-canton:

Implementing Multi-Sig in Canton
================================

Introduction
------------

In the Canton protocol, at a high level, there are two categories of operations that require signatures:
* Daml transactions, which consist of submissions and confirmations
* Topology transactions, which modify the network's configuration.

Submissions are a request to change on-ledger state, submitted by one or more parties.
Confirmations are approvals by counter-parties to a submitted transaction that certify the transaction is valid and can be committed.
Finally, topology transactions modify the shared global topology state of the network:
they determine which Validators host a party, what rights that party grants to the Validator (submission, confirmation, or observation),
and which public-private keypair the party will use to authorize Daml transactions.

The idea of multi-sig, or decentralization, where multiple signatures are required on an action,
can be applied to each of these types of transactions.
This document walks you through the options for setting up:

1. A decentralized namespace (to apply decentralized control over topology transactions)
2. Decentralized party hosting (for decentralizing transaction validation & confirmation)
3. Decentralized signing of transaction submissions for a given party,
   including Daml models for achieving multi-sig workflows for Daml transactions, and multi-signature submissions using external parties.

1: Decentralizing Control Over Topology Changes
-----------------------------------------------

The root of trust in Canton is the private transaction signing key (or keys),
which generate a signature for a transaction, proving that a given party has authorized that transaction.
The transactions that *associate signing keys with parties* are an example of *topology* transactions.
The transactions that give a Validator node permission to create and store data on behalf of a party
(confirmation and observation rights) are *also* examples of topology transactions.

So the first level of decentralization in Canton is whether or not to decentralize control over topology transactions for a given party.
If a single private-public keypair controls this association,
then a signature from the private key in that keypair can change the signing keys for that party.

If the association of keys to a party requires signatures from more than one set of keys,
then control over the party itself is decentralized.
We refer to decentralization of control over the signing keys for a party as a "decentralized namespace":
that is, a decentralized namespace maps party IDs to keys without relying on a single authority.

Creating A Decentralized Namespace
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Setting up a decentralized namespace involves two steps:

1. Namespace Delegation

Create a ``NamespaceDelegation`` transaction for each owner. Each transaction registers a public key that can participate in authorizing changes to the decentralized namespace.

2. Decentralized Namespace formation

The ``DecentralizedNamespaceDefinition`` transaction refers to the N keys established in the prior step
and defines a validity threshold T ≤ N.

For a more detailed explanation see :ref:`decentralized-parties`.

Creating and Hosting a party in (controlled by) the Decentralized Namespace
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ``PartyToParticipant transaction`` states where the party will be hosted.
This transaction serves three distinct purposes:

1. Authorizes party hosting:
   The ``PartyToParticipant`` transaction states where the node on behalf of the party will be hosted and with what permission (submission, confirmation, observation)

   * This must be signed by at least T out of the N namespace owners

2. Registers keys: The party's protocol signing key (or keys) can and should be registered in the ``PartyToParticipant`` transaction as well.
3. Proves possession: This transaction must also be signed by the protocol keys it registers, to prove ownership of the corresponding private keys.

With the above transactions in hand and the corresponding signatures,
these topology transactions may be submitted to the
:externalref:`API documentation <com.daml.ledger.api.v2.admin.PartyManagementService.AllocateExternalParty>`
endpoint on the Ledger API, which creates the decentralized party.
All of the hosting nodes specified in the transaction must approve the transaction before it will take effect.

In this setup, the party is secured at two distinct threshold layers:

1. Identity Control: The party's namespace is governed by the namespace owners via the ``DecentralizedNamespaceDefinition`` (requiring T signatures)
2. Operational control: Authorization of Daml transactions is governed by the protocol keys defined in the ``PartyToParticipant`` transaction.

Whether or not you decentralize control over topology transactions via a decentralized namespace,
you then have the option to use one or more signing keys to sign submissions by a given party.
We'll cover this in section 3.

For further details, see:

* :ref:`decentralized-parties`
* :ref:`tutorial_onboard_external_party`
* :ref:`overview_canton_external_parties`

2: Decentralizing Confirmations
-------------------------------

Due to the privacy model in Canton,
every transaction needs to be confirmed by the participants hosting the parties that are stakeholders on the transaction.
The ``partyToParticipant`` mapping in the topology state defines which participant(s) host every party in the network.

A party with more than one Validator in the ``partyToParticipant`` mapping is called a *multi-hosted party.*
When given a threshold higher than 1 for confirmations, the multi-hosted party is referred to as a *decentralized party*.

Every transaction requiring confirmation from this party is sent to all participants listed in its ``partyToParticipant`` mapping,
and once the number of confirmations from different participants crosses the threshold, the transaction is considered confirmed by the party.

For details on confirming participant nodes see :ref:`overview_canton_external_parties_cpn`.

3: Decentralizing Control over Command Submission
-------------------------------------------------

Canton ledger state is changed by submitting commands through a participant, which expands the commands into Daml transactions.
Each command submission is signed by the party (or parties) that authorize and submit this command.
For a standard (non-decentralized) party in Canton, this is simple: that party is hosted on a participant node, and submits the command via that node.
For a decentralized party, this becomes more complicated,
as one needs to figure out who (i.e. which user, via which participant) performs the actual submission,
and how do they prove authority from a threshold of users to act on behalf of the decentralized party.

There are three main possible approaches for submissions on behalf of a decentralized party, each with its own pros and cons:

1. Collecting authority via delegation, using Daml workflows
2. In-Daml signature verification
3. External signing with multi-signatures

Collecting Authority via Delegation Daml Workflows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A common pattern in Daml is delegating authority to execute actions on behalf of other parties.
For example, a propose-accept workflow could consist of multiple steps that collect the delegation from the sender and receiver of an asset to perform a certain transaction.
Depending on the exact workflow, either party can then submit a transaction for the transfer with the authority of the other party.

This pattern can be leveraged for delegating authority to "simple",
i.e. non-decentralized parties to submit transactions that the decentralized party will then confirm.
In this case, you will typically have one "rules" contract with the decentralized party as its signatory,
and choices in that contract that define actions that different actors can perform.
The "rules" will then typically include a list of parties that can take the actual actions, and under which conditions.

In the case of the Canton Coin app, for example, the ``dsoParty`` is a decentralized party that is the signatory on the `dsoRules contract <https://github.com/hyperledger-labs/splice/blob/0f194d4e5b55722cfd14b82a74e4b0db47f0f251/daml/splice-dso-governance/daml/Splice/DsoRules.daml#L441>`__).
The contract has a list of Super Validators (the ``svs`` `field <https://github.com/hyperledger-labs/splice/blob/0f194d4e5b55722cfd14b82a74e4b0db47f0f251/daml/splice-dso-governance/daml/Splice/DsoRules.daml#L444>`__ in the contract) that participate in the application.

Certain actions can then be taken by any SV, assuming certain conditions hold.
These conditions are enforced via assertions in the Daml workflows themselves.
For example, any SV can execute the `DsoRules_Amulet_Expire choice <https://github.com/hyperledger-labs/splice/blob/0f194d4e5b55722cfd14b82a74e4b0db47f0f251/daml/splice-dso-governance/daml/Splice/DsoRules.daml#L1095C34-L1095C43>`__
to archive an Amulet contract for which the expiration date has already passed.

Other actions require collecting enough confirmations from other SVs.
This is also implemented in Daml.
For example, any SV can invoke the `DsoRules_RequestVote choice <https://github.com/hyperledger-labs/splice/blob/0f194d4e5b55722cfd14b82a74e4b0db47f0f251/daml/splice-dso-governance/daml/Splice/DsoRules.daml#L674C34-L674C43>`__.
Any SV can then cast a vote on it via invoking the `DsoRules_CastVote choice <https://github.com/hyperledger-labs/splice/blob/0f194d4e5b55722cfd14b82a74e4b0db47f0f251/daml/splice-dso-governance/daml/Splice/DsoRules.daml#L712>`__.
Any SV can then also invoke the `DsoRules_CloseVoteRequest choice <https://github.com/hyperledger-labs/splice/blob/0f194d4e5b55722cfd14b82a74e4b0db47f0f251/daml/splice-dso-governance/daml/Splice/DsoRules.daml#L761>`__ to complete the process.
Under the conditions encoded in Daml (e.g. enough "Yay" votes, and a certain time has been reached), the action voted on may be taken by the ``dsoParty``.
Note that all submissions in the process were performed by "simple", non-decentralized, parties, the SV parties in the example of Canton Coin.

We will return to the bootstrapping problem of how to create the "rules" contract owned by the decentralized party in the first place toward the end of the document.

**Pros of this approach:**

* All actions (specifically, votes and action submissions) are visible as separate on-ledger transactions.
* It does not require any off-ledger communication between the operators of the individual nodes hosting the decentralized party.
  They observe each other's actions on-ledger and have all the information required to act accordingly on-ledger.

**Cons of this approach:**

* Since every step (e.g. every vote) is an on-ledger transaction, it is relatively expensive in traffic on the synchronizer.
  Depending on the frequency of operations, this may be impractical.

In-Daml Signature Verification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this approach, as in the previous approach, a single party (which is *not* the decentralized party) submits a command on behalf of the decentralized party.
However, the process of collecting signatures is carried out off-ledger.
The collected signatures are submitted as arguments to a Daml choice,
and signature verification is implemented as Daml assertions.

An example for a Daml choice asserting validity of a signature can be found in the `Daml test code <https://github.com/digital-asset/daml/blob/main/sdk/daml-script/test/daml/crypto/AttestationTests.daml>`__.
This can then be extended to a choice that asserts existence and validity of a threshold of signatures.
It will still be a choice on a "rules" contract owned by the decentralized party,
where the controller of the choice is another party,
but the process of collecting the signatures (or "votes") is no longer implemented in this contract.

The process for collecting signatures and submitting a transaction via such a choice is:

1. Each member (controller of a signing key) signs some agreed upon "encoded command" off-ledger. e.g. some agreed upon JSON struct that encodes the details of the command to execute.
2. The signatures are collected via some off-ledger channel to one node that submits the transaction.
3. The submitting node constructs a single Daml transaction via the choice described above, including the collected signatures as arguments.
4. The assertions in the Daml code confirm validity, and the action is executed.

**Pros of this approach:**

* Submitted signatures are recorded on-ledger.
* The Daml workflows are simpler than delegation patterns. Signature collection (e.g. "voting") happens off-ledger.
* Cheaper on on-ledger traffic
* Cheaper on-ledger traffic compared to full voting workflows. Enables workflows where the authorizing signature may be given long before submission time, e.g., air-gapped offline signing

**Cons of this approach:**

* Requires an additional off-ledger channel for collecting signatures to ensure they are not lost
* Only submitted signatures are recorded. Signatures that are collected but not submitted are not recorded on-ledger
* To prevent replay attacks, signatures should cover state-specific data (e.g., contract IDs, nonces). Signatures may become invalid if the referenced state changes before submission.

External multi-sig signing of Daml transaction submissions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can configure an external party to require a threshold of signatures (multi-sig) to submit Daml transactions.
This setup moves the coordination of signatures off-ledger while keeping the on-ledger Daml code simpler.

Start by associating multiple protocol signing keys to the Party, via a the PartyToParticipant topology transaction.
This configuration dictates:

* **Hosting:** Which nodes host the party.
* **Submission Keys:** A list of public keys (Protocol Signing Keys) and a required threshold (e.g., 3 of 5) to authorize submissions.

The process for preparing and submitting a transaction requiring multiple external signatures:

* **Prepare:** A participant node prepares the transaction payload (subject to a global timeout, typically 24h).
* **Distribute:** The payload is sent off-band to the required signers.
* **Sign:** Each member validates and signs the payload locally.
* **Collect & Submit:** Once signatures are collected off-ledger, they are submitted along with the prepared transaction to a participant node. When the threshold defined in ``PartyToParticipant`` is met the transaction is submitted to the ledger.

A prepared transaction is subject to a timeout (currently 24 hours on the Global Synchronizer), 
after which submissions will fail.

When using external multi-sig signing with a threshold greater than one, the system provides 
resilience against key compromise. If a single key is stolen or misused, the attacker cannot 
authorize transactions on their own. They would need to compromise enough keys to meet the threshold.

However, external multi-sig signing alone does not protect against a malicious hosting node that 
might ignore signature requirements when submitting transactions. To guard against this, the party 
should also be **Multi-Hosted** (as described in Section 2) with a confirmation threshold greater than one. 
This guarantees that multiple independent nodes must verify the external signatures before the transaction 
is confirmed. For a detailed discussion of trust assumptions for external parties, see :ref:`party_trust_model`.

**Pros of this approach:**

* Simpler Daml – No changes to Daml models are required; the party acts like any other entity in the code.
* Standard Identity – The decentralized nature is abstracted away from the application logic.

**Cons of this approach:**

* Complex Coordination – Requires robust off-ledger infrastructure to distribute payloads and collect signatures.
* Complex Validation – Signers must decode and inspect raw transaction binaries before signing.
* Signatures in API stream – Individual signatures are available via the Ledger API transaction update stream, but not stored in Daml contract state.

Summary
~~~~~~~

The choice between these approaches depends on your infrastructure and visibility requirements.
Delegation workflows require more complex Daml coordination logic but operate entirely on-ledger
without needing off-ledger communication channels between nodes. In-Daml signature verification
simplifies the Daml logic while still recording signatures in contract state, though it requires
an off-ledger channel to collect signatures before submission. External multi-sig signing requires
the least Daml complexity. The party behaves like any standard party in your contracts, but demands
robust off-ledger coordination to prepare, distribute, and collect signatures on transaction payloads.

Regardless of which approach you choose, all transaction signatures are accessible via the
Ledger API transaction update stream. The distinction is whether signatures also appear in
contract state (as with the Daml-based approaches) or only in the protocol layer (as with
external multi-sig signing). For implementation details,
see :ref:`howto_decentralized_parties` and
:externalref:`external submission docs <tutorial_externally_signed_transactions_part_2>`.

Additional Topics
=================

Bootstrapping Multi-sig Daml Workflows
--------------------------------------

The Daml-based approaches described above in "Collecting Authority via Delegation Daml Workflows" and "In-Daml Signature Verification"
both require an initial "rules" contract where the coordinating party is a signatory.
The multi-sig governance in these approaches is enforced by the Daml code within the contract itself,
not by protocol-level signing requirements on the party.

The party creating the initial contract does not need multi-sig external signing configured.
A party with a single protocol signing key can submit the transaction that creates the rules contract.
Once created, the Daml logic in the contract governs what actions require multiple approvals.

If trust requirements demand that the initial contract creation requires multiple parties to agree,
use the External multi-sig signing approach as described in "External multi-sig signing of Daml transaction submissions".

Bootstrapping is independent of whether the party is multi-hosted for confirmations or the
party's namespace requirements regarding signatures for topology changes.

See :ref:`howto_decentralized_parties` for more information on setting up decentralized namespaces, multi-hosted confirmations, and external signing keys.

Key Management
--------------

Depending on your setup, there are between four sets of keys that might be used in the above.
Any given setup will use either two or three of these sets of keys.

* The participant-internal keys, which the participants use to sign confirmations
* Namespace member keys, which are used to sign topology transactions. It is highly recommended to make these separate keys, as demonstrated in :externalref:`Setup a decentralized party <howto_decentralized_parties>`.
* If external signing is in use, then keys for external signing are also required. It is typically acceptable for these to reuse the same keys as the namespace keys, but they can also be made different for extra security.
* Similarly, if in-Daml signature verification is used, then keys for this signing are required. In-Daml signatures may reuse the same keys as the namespace keys, but they can also be different keys, for added security.

Risks Related to Topology Changes
---------------------------------

Hosting a party on an additional node with confirmation rights (Onboarding to an additional node)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Changing the set of participants hosting the decentralized namespace and party,
and the corresponding thresholds, can be performed through a process called
:ref:`offline-party-replication`.

Refer to :ref:`party-replication` for more information.

Shutting down a node that hosts a party with (submission, confirmation, observation?) rights
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is safe to completely shut down ("offboard" or "decommission") a Validator node that hosts one or more participating in a decentralized confirmation pool.

It is also safe to bring this node onto the network within the sequencer pruning window, which is, for example, 30 days on the Global Synchronizer.
After the sequencer pruning window, a number of failures could occur which currently have no known resolutions.

Impact on Malicious Hosting Participants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To learn more about the impact of malicious or faulty nodes hosting a multi-sig party, refer to :ref:`party_trust_model` and :ref:`troubleshoot-commitments`.

For further reading, refer to :ref:`decentralization`.
