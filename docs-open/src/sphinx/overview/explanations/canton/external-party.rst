..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _overview_canton_external_parties:

##########################
Local and external parties
##########################

In Canton, a **party** represents an entity capable of creating and interacting with contracts on the ledger.
Each party is hosted on one or more **Participant Nodes**, which act as intermediaries between the party and the ledger.
Different kinds of interactions require the appropriate authentication and authorization from the party.
This page explains how a party integrates and interacts with the Canton Protocol, the trust relationships involved, and their implications.

.. tip::

    For answers to common questions about signing keys and authorization, check out the :ref:`FAQ <overview_canton_external_parties_faq>` section at the end of this page.

Hosting relationship
********************

A hosting relationship for a party specifies the permissions for each participant node and the confirmation threshold.

A Participant Node can have different permissions on behalf of a party it is hosting.

.. _overview_canton_external_parties_spn:

Submitting participant node
===========================

A node with submission permissions is called a **Submitting Participant Node** (SPN).
A party entrusts its SPNs to unilaterally authorize the submission of Daml transactions to the ledger on their behalf.
Each SPN uses its own private key to sign and authorize transactions on the party's behalf.
This is the strongest permission available for participant nodes; SPNs by definition are also CPNs and OPNs (see below).

.. _overview_canton_external_parties_cpn:

Confirming participant node
===========================

A node with confirmation permissions is called a **Confirming Participant Node** (CPN).
A party entrusts its CPNs to confirm and authorize valid transactions to be committed to the ledger on their behalf.
CPNs can confirm transactions by signing a confirmation response using their own private key. They cannot initiate transactions for the party.
CPNs are by definition OPNs (see below).
CPNs are configured with a threshold. The Canton protocol requires at least threshold confirmations amongst the party CPNs for a valid transaction to be committed to the ledger.
This provides additional resilience and security for the party.
A single CPN with a threshold of 1 is the simplest scenario.

An immediate consequence of this is a tradeoff between availability and security:

* the higher the threshold, the higher the security but the lower the availability guarantees: it takes fewer offline CPNs to block transactions from being approved
* the lower the threshold, the higher the availability guarantees but the lower the security: it takes fewer malicious CPNs to approve an invalid transaction

.. note::

    Due to Canton's privacy properties, only CPNs and SPNs can reliably enforce that the submitting node has submission permissions.
    This means that in principle, a malicious CPN could attempt to submit a transaction on behalf of a party even though it lacks submission permissions.
    Opting for multiple hosting CPNs with a threshold > 1 is an effective mitigation against that risk.
    It also effectively renders submission on behalf of that party impossible if the party is local, as no SPN can singlehandedly authorize submission of the transaction.

Observing participant node
===========================
A node with observation permissions is called an **Observing Participant Node** (OPN).
A party entrusts its OPNs to faithfully record and provide read access to transactions involving them on the ledger.
Nodes with only observation permissions cannot submit nor confirm transactions on behalf of the parties they host.

Submission key holder
*********************

A party which holds and controls its own submission signing keys is a submission key holder.
Canton supports the registration of multiple submission signing key pairs along with a threshold.
The associated private signing keys are only controlled and usable by the party.
The party is responsible for managing and maintaining their private keys.
How to do so is left to the party operator or client application and is outside of the scope of Canton.
Typically users may use a crypto custody provider or other cryptographic key management service to secure their keys.

In the simplest case, a single key pair with a threshold of ``1`` enables the submission of transactions through a single signature.
More complex processes may require multiple signatures; in that case submission key holders can register several (unique) key pairs with a higher threshold.
The Canton protocol guarantees that at least a threshold number of valid signatures from different keys must be provided with the transaction in order to authorize it on the ledger.
Those keys and associated threshold are defined in the ``PartyToKeyMapping`` topology transaction.

.. literalinclude:: CANTON/community/base/src/main/protobuf/com/digitalasset/canton/protocol/v30/topology.proto
   :language: protobuf
   :start-after: [doc-entry-start: PartyToKeyMapping]
   :end-before: [doc-entry-end: PartyToKeyMapping]

Namespace
*********

Namespaces are a concept in Canton's :externalref:`Topology <topology-namespaces>`.
It is relevant for parties because every party exists within a namespace, which is derived from the public key of the root signing key pair of that namespace.
Note that every canton node also defines a namespace for itself.
The namespace essentially defines the identity of the party or node on the network.

Local party
***********

A local party is defined as a party with one or more SPNs. It shares its namespace with one of those SPNs, and consequently delegates
its topology management to the SPN operator.
Local parties are the simplest way to interact with a Canton ledger and the one that puts the most trust in the hosting nodes.
They're usually best suited to situations when automation is required to run commands on behalf of the party, as a SPN has full authority to submit the transactions
without requiring explicit approval.

External party
**************

An external party is defined as a submission key holder, with **no** SPN, and with its own unique namespace,
controlled by its own signing key (as opposed to local parties who exist under their SPN's namespace).
The ``PartyToKeyMapping`` topology transaction allows the party to submit transactions to the ledger using its own keys.
Crucially, it does **not** grant **submission permission** to any node.
This removes potential regulatory burdens from a Participant Node by removing the Participant Node's ability to act on the ledger without the party's explicit approval.
An external party still requires **at least one CPN** to confirm transactions on its behalf and record its ledger state.
This keeps the latency of the network reasonable and provides a trusted source for the party to record its activity on the ledger.

Local vs External party hosting
*******************************

The following table summarizes the hosting relationship for local and external parties with the different node permissions.

+-------+-------------+----------------+
|       | Local Party | External Party |
+=======+=============+================+
| # SPN | at least 1  | 0              |
+-------+-------------+----------------+
| # CPN | any number  | at least 1     |
+-------+-------------+----------------+
| # OPN | any number  | any number     |
+-------+-------------+----------------+

Submission Flow
***************

The difference between local and external parties is that for external parties, the private keys used to authorize transactions are solely controlled by the party (for local parties, the SPN controls those keys).
Instead external parties explicitly sign each transaction submission with their private key.
More specifically they sign a :externalref:`hash <external_signing_hashing_algo>` of the transaction tree that accurately represents all ledger effects that the transaction will have if it is committed successfully to the ledger.
Generating this transaction requires, among other things, complex logic, a Daml interpretation engine, up-to-date topology, and ACS knowledge.
To simplify the process, the submission flow for external parties is split in two steps:

* **Preparation**: Transform a Ledger API command into a Daml transaction.
    This step can be performed by any Participant Node connected to the synchronizer of choice for the transaction and the packages required by the transaction.
    Such node is called a **Preparing Participant Node (PPN)**.
    A Ledger API user with `readAs` scope for the party is required to prepare transactions on behalf of the party.
* **Execution**: The transaction and its signature are sent to a Participant Node connected to the synchronizer on which the transaction is to be executed.
    This Participant Node simply forwards the transaction to the synchronizer.
    Such node is called an **Executing Participant Node (EPN)**.
    A Ledger API user with `actAs` scope for the party is required to execute transactions on behalf of the party.

That's for the submission path. On the read path (observing transactions committed to the ledger), external parties can read their transaction stream from CPNs or OPNs.
For additional security they can read from several and cross compare the results with a threshold to achieve byzantine fault tolerance.

The following sequence diagram describes the submission flow both for local and external parties, highlighting differences:

* Interactions in a red background are exclusive to external parties
* Interactions in a blue background are exclusive to local parties
* Interactions with no background are common to both local and external parties

.. mermaid::

    sequenceDiagram
        actor Alice
        participant PPN
        participant EPN
        note over PPN,EPN: For local parties,<br/>PPN == EPN == SPN<br/>(with submission permission)
        Alice->>+PPN: Ledger API Command
        rect rgb(255, 153, 153)
        PPN->>+Alice: - Transaction Data<br/>- Transaction Hash
        Note over Alice: Inspects transaction data
        Note over Alice: Validates transaction hash
        Note over Alice: Signs transaction hash
        Alice->>+EPN: - Transaction Data<br/>- Transaction Signature
        end
        note over EPN: From there on the flow for<br/>local and external parties<br/>is the same
        participant Synchronizer
        participant CPN 1
        participant CPN 2
        participant OPN
        Note over CPN 1,CPN 2: Host Alice w/ Confirmation rights
        EPN->>+Synchronizer: Confirmation Request
        Synchronizer->>+EPN: Confirmation Request
        Synchronizer->>+CPN 1: Confirmation Request
        Synchronizer->>+CPN 2: Confirmation Request
        Synchronizer->>+OPN: Confirmation Request
        Note over OPN: OPNs do not send confirmation responses<br/>but receive the confirmation request...
        Note over EPN: For local parties,<br/>the SPN sends a confirmation response as<br/>it has confirmation permission for the party.
        Note over EPN: For external parties,<br/>the EPN also sends a confirmation response<br/>but only as a veto mechanism to provide<br/>consistent completion events.
        EPN->>+Synchronizer: Confirmation Response
        rect rgb(255, 153, 153)
        Note over CPN 1: Validate Alice's transaction signature
        end
        CPN 1->>+Synchronizer: Confirmation Response
        rect rgb(255, 153, 153)
        Note over CPN 2: Validate Alice's transaction signature
        end
        CPN 2->>+Synchronizer: Confirmation Response
        Synchronizer->>+CPN 1: Transaction Verdict
        Synchronizer->>+CPN 2: Transaction Verdict
        Synchronizer->>+OPN: Transaction Verdict
        Note over OPN: ...and the verdict
        Synchronizer->>+EPN: Transaction Verdict
        Note over Alice: Listens to completion events<br/>to learn about the<br/>outcome of the transaction.
        EPN->>+Alice: Completion event
        Note over Alice: Queries CPNs for transaction stream,<br/> active contract states.
        CPN 1->>+Alice: Transaction stream, ACS
        CPN 2->>+Alice: Transaction stream, ACS
        Note over EPN: For local parties, SPN can also provide<br/>ledger data.
        rect rgb(153, 204, 255)
        EPN->>+Alice: Transaction stream, ACS
        end

Limitations
***********

* **Local parties**:

  * If the participant threshold > 1, the party cannot submit transactions, only confirm transactions for which it is a stakeholder.

* **External parties**:

  * Single root node: Only transactions with a single root node are supported.
  * Single submitting party: Only transactions requiring authorization from a single party are supported.

* **Both local and external**:

  * Command completion for an individual submission is only available on the SPN (EPN for external parties) used for the submission
  * Command deduplication for an individual submission is only available on the SPN (EPN for external parties) used for the submission

.. _party_trust_model:

Trust model
***********

Definitions
===========

* **User**: Leger API user with permission to ``actAs`` the party. Is assumed to act faithfully according to the party's intents on command submissions.
* **Party owner**: Individual or entity with control over the party's namespace keys. Is assumed to act faithfully according to the party's intents on party governance.

Trust relationships
===================

* **SPN**: The user fully trusts the SPN.
  For example, the SPN can submit and authorize any transaction for which the local party is a required authorizer without the user's approval.
* **CPN**:

  * The user trusts that less than threshold CPNs incorrectly approve or reject requests on behalf of the party they host as expected according to the Canton protocol.
    If however threshold many CPNs are malicious, they can incorrectly approve an invalid transaction. This includes transactions with an invalid external signature for external parties.
  * The user trusts that less than threshold CPN operators vet malicious or invalid Daml packages.
    In particular, honest CPN operators must compare new packages with older versions of those packages if there are active contracts in the Participant Node's ACS that have been created with the older package versions.
    In such case, honest CPN operators must obtain approval of the contract signatories before vetting the new package version.
    If however threshold many CPN operators vet a malicious package, arbitrary smart contract code can be run on new or existing contracts.
* **PPN**: The user does not trust the PPN.
* **EPN**: The user does not trust the EPN, except when:

    * Acting on command completions obtained from it
    * Using the Time To Live (TTL) feature via the :externalref:`max_record_time <com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.max_record_time>` field.

For example, the EPN can intentionally emit incorrect completion events, leading users to retry a submission thinking it had failed when it had actually succeeded.
It can also ignore or modify the ``max_record_time`` field in which case it will not be enforced according to the user's intent.

User responsibilities
=====================

The user must follow the below guidelines when interacting with participant nodes for command submission:

* **SPN and CPN**: Party owners carefully choose their SPNs and CPNs.
* **PPN**: Users visualize the prepared transaction from the PPN.
    They check that it matches their intent. They also validate the data therein to ensure that the transaction can be securely submitted. In particular:

  * The transaction corresponds to the ledger effects the User intends to generate
  * The :externalref:`preparation time <com.daml.ledger.api.v2.interactive.Metadata.preparation_time>` is not ahead of the current sequencer time on the synchronizer. A reliable way to do this is to compare the `preparation_time` with the minimum of:

      * Wallclock time of the submitting application
      * Last record time emitted by at least participant threshold CPNs + the configured `mediatorDeduplicationTimeout` on the synchronizer

    * If :externalref:`min_ledger_time <com.daml.ledger.api.v2.interactive.PrepareSubmissionRequest.min_ledger_time>` is defined in the prepare request, validate that
      the :externalref:`min_ledger_effective_time <com.daml.ledger.api.v2.interactive.Metadata.min_ledger_effective_time>` in the response is either empty or ahead of the `min_ledger_time` requested.

    * Users compute the hash of the transaction according to the specification described :externalref:`here <external_signing_hashing_algo>`.
        The hash provided in the PPN's response must be ignored unless the PPN is trusted.
* **EPN**:

  * Users sign the hash they computed from the Daml transaction
  * Keys used to sign the transaction are only used for this purpose
  * Keys used for signature are stored securely and kept private
  * Users do not rely on :externalref:`submission ID <com.daml.ledger.api.v2.Completion.submission_id>` in command completions

Mitigations
===========

**Malicious CPNs**:
The party owner chooses multiple CPNs to host it jointly in a fault-tolerant mode by setting the participant threshold to more than one.
The threats are mitigated if fewer CPNs than the threshold are faulty.
Canton achieves fault tolerance by requiring approvals from participant threshold many CPNs for transactions that are to be committed to the ledger.
Similarly for rejections, at least threshold many rejections must be reached for a transaction to be rejected.
If the party consumes information from CPNs such as the transaction stream, it must cross-check the information from participant threshold many different CPNs to achieve fault tolerance.

**Malicious EPNs**:
Malicious EPNs can intentionally emit incorrect completion events.
There is no practical general mitigation at this time.
However, self-conflicting transactions are not subject to this threat, for example when
the transaction contains an archive on at least one contract the submitting party is a signatory of.
In that case the CPN(s) then reject resubmissions of the transaction.
Users can attempt to correlate completion events with the transaction stream from the (trusted) CPN.
Note the transaction stream only emits committed transactions, making this difficult to leverage in practice.

.. _overview_canton_external_parties_faq:

FAQ
***

What are the cryptographic keys used by local parties?
======================================================

All keys used by local parties for authorization are managed by the party's hosting node(s):

    - The `submitting node's <overview_canton_external_parties_spn>` protocol key to authorize :externalref:`submission of transactions <topology-cryptographic-keys>`
    - The `submitting node's <overview_canton_external_parties_spn>` namespace key to authorize :externalref:`namespace management <topology-namespaces>`
    - The `confirming node's <overview_canton_external_parties_cpn>` protocol key to authorize confirmation of valid transactions

What are the cryptographic keys used by external parties?
=========================================================

    - The party's own key(s) to authorize :externalref:`submission of transactions <topology-cryptographic-keys>` and :externalref:`namespace management <topology-namespaces>`
    - The `confirming node's <overview_canton_external_parties_cpn>` key to authorize confirmation of valid transactions

How does multi-hosting affect external parties?
===============================================

The core difference for multi-hosting of external parties is that their namespace is not shared with any of the hosting nodes.
This implies that topology transactions involving the external party must be signed using the external namespace key of the party.
This includes the party-to-participant mapping which configures the hosting of the party.

Refer to the :externalref:`multi hosted parties <multi-hosted-parties>` documentation for more details.

Additional resources
********************

- :externalref:`SDK External Signing Overview <sdk_external_signing_overview>`
- :externalref:`External Party Onboarding Tutorial <tutorial_onboard_external_party>`
- :externalref:`External Party Transaction Submission Tutorial <tutorial_externally_signed_transactions>`
- :externalref:`External Signing Transaction Hash <external_signing_hashing_algo>`
