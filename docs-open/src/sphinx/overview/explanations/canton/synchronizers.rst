..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _synchronizer-architecture:

*************
Synchronizers
*************

.. wip::
   A deep look into the synchronizer architecture; functional requirements have already been established in protocols.rst

.. _synchronizers-sequencer:

The Sequencer
=============

.. todo:: <https://github.com/DACH-NY/canton/issues/25653>
   See also https://github.com/DACH-NY/canton/blob/main/docs/src/sphinx/arch/canton/sequencing.rst


   * Members; envelopes and recipients including projection (confidential delivery)

   * Ordering guarantees

     * Ordering as a separate protocol layer (see below)

   * Traffic management

   * Traffic credits managed by the sequencers and topped up by sequencer operators

   * Source of time

     * Ledger time, submission time (rename to preparation time???), skews

..
   .. _time-in-canton:

   Time in Canton
   ^^^^^^^^^^^^^^

   The connection between time in Daml transactions and the time defined in Canton is
   explained in the respective `ledger model section on time <https://docs.daml.com/concepts/time.html#time>`__.

   The respective section introduces `ledger time` and `record time`. The `ledger time` is the
   time the participant (or the application) chooses when computing the transaction prior
   to submission. We need the participant to choose this time as the transaction is pre-computed
   by the submitting participant and this transaction depends on the chosen time. The `record time`
   is assigned by the sequencer when registering the confirmation request (initial submission
   of the transaction).

   There is only a bounded relationship between these times, ensuring that the `ledger time` must be
   in a pre-defined bound around the `record time`. The tolerance is defined on the synchronizer
   as a synchronizer parameter, known to all participants:

   .. code-block:: bash

      canton.synchronizers.mysynchronizer.parameters.ledger-time-record-time-tolerance

   The bounds are symmetric in Canton, so the Canton synchronizer parameter ``ledger-time-record-time-tolerance`` equals
   the ``skew_min`` and ``skew_max`` parameters from the ledger model.

   .. note::

      Canton does not support querying the time model parameters via the Ledger API, as the time model is
      a per-synchronizer property and this cannot be properly exposed on the respective Ledger API endpoint.

   Checking that the `record time` is within the required bounds is done by the validating participants
   and is visible to everyone. The sequencer does not know the `ledger time` and therefore cannot perform
   this validation.

   Therefore, a submitting participant cannot control the output of a transaction depending on `record time`,
   as the submitting participant does not know exactly the point in time when the transaction will be timestamped
   by the sequencer. But the participant can guarantee that a transaction will either be registered before a
   certain record time, or the transaction will fail.



The Mediator
============

.. todo:: <https://github.com/DACH-NY/canton/issues/25653>

   Mediator = Two-phase commit coordinator

   * Obtain list of expected quorums (currently it's a tree)

     * Deduplicate requests within bounded periods


The Ordering Layer
------------------

.. todo:: <https://github.com/DACH-NY/canton/issues/25653>
   * API spec

   * Overview of integrations with links

     * CometBFT

     * DA BFT (once we have docs on this)

     * DB sequencer (once we have this ready)

   * No mention of Fabric / Besu


.. todo:: <https://github.com/DACH-NY/canton/issues/25653>
   * Remove all implementation details (in particular internal architecture)

     * If suitable, move them to the "subnet" chapter.

..
  Synchronizer Entities
  ---------------------

  A Canton synchronizer consists of three entities:

  - the sequencer
  - the mediator
  - and the **topology manager**, providing a PKI infrastructure, and party
    to participant mappings.

  We call these the **synchronizer entities**. The high-level communication
  channels between the synchronizer entities are depicted below.

  .. https://www.lucidchart.com/documents/edit/b22cd15e-496e-41cb-8013-89fd1f42ab34
  .. image:: ./images/overview/canton-synchronizer-diagram.svg
     :align: center
     :width: 80%

  In general, every synchronizer entity can run in a separate trust domain
  (i.e., can be operated by an independent organization). In practice,
  we assume that all synchronizer entities are run by a single organization
  and that the synchronizer entities belong to a single trust domain.

  Furthermore, each participant node runs in its own trust domain.
  Additionally, the participant may outsource a part of its identity management infrastructure, for example to a
  certificate authority.
  We assume that the participant trusts this infrastructure, that is, that the participant and its identity management belong
  to the same trust domain.
  Some participant nodes can be designated as **VIP nodes**, meaning
  that they are operated by trusted organizations. Such nodes are important
  for the VIP confirmation policy.

  The generic term **member** will refer to either a synchronizer or a participant node.

  .. _sequencer-overview:

  Sequencer
  ^^^^^^^^^

  We now list the high-level requirements for the sequencer.

  **Ordering:** The sequencer provides a `global total-order
  multicast <http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.85.3282&rep=rep1&type=pdf>`__
  where envelopes are uniquely timestamped and the global ordering is
  derived from the timestamps. Instead of delivering a single envelope, the
  sequencer provides batching, that is, a
  list of individual envelopes is submitted. All of these envelopes get the
  timestamp of the batch they are contained in. Each envelope may
  have a different set of recipients; the envelopes in each recipient's batch
  are in the same order as in the sent batch.

  **Evidence:** The sequencer provides the recipients with a
  cryptographic proof of authenticity for every batch it
  delivers, including evidence on the order of envelopes.

  **Sender and Recipient Privacy:** The recipients
  do not learn the identity of the submitting participant.
  A recipient only learns the identities of recipients
  on a particular envelope from a batch if it is itself a recipient of that
  envelope.
  
  .. note::
     In the implementation, the recipients of an envelope are not a set of members (as indicated above),
     but a forest of sets of members.
     A member receives an envelope if it appears somewhere in the recipient forest.
     A member sees the nodes of the forest that contain itself as a recipient (as well as all descendants of such nodes),
     but it does not see the ancestors of such nodes.
     This feature is used to support bcc-style addressing of envelopes.

  .. _mediator-overview:
  
  Mediator
  ^^^^^^^^

  The mediator's purpose is to compute the final result for a
  confirmation request and distribute it to the participants,
  ensuring that transactions are atomically committed across
  participants, while preserving the participants' privacy,
  by not revealing their identities to each other.
  At a high level, the mediator:

  - collects mediator responses from participants,
  - validates them according to the Canton protocol,
  - computes the mediator verdict (approve/reject/timed out) according to the confirmation policy, and
  - sends the result message.

  For auditability, the mediator also persists every finalized request
  together with its verdict in long-term storage
  and allows an auditor to retrieve messages from this storage.



  Recall the high-level topology with Canton synchronizer being backed by different
  technologies, such as a relational database as well as block-chains like
  Hyperledger Fabric or Ethereum.

  .. https://app.lucidchart.com/documents/edit/da3c4533-a787-4669-b1e9-2446996072dc/0_0
  .. figure:: images/topology.png
     :align: center
     :width: 80%

  In this chapter, we define the requirements specific to a Canton synchronizer, explain
  the generic synchronizer architecture, as well as the concrete integrations for Canton
  synchronizer.

  Synchronizer-Specific Requirements
  ==================================

  The :ref:`high-level requirements <requirements>` define requirements for Canton
  in general, covering both participants and synchronizers. This section categorizes and
  expands on these high-level requirements and defines synchronizer-specific
  requirements, both functional and non-functional ones.

  Functional Requirements
  -----------------------

  The synchronizer contributes to the high-level functional requirements in terms of
  facilitating the synchronization of changes. As the synchronizer can only see
  encrypted transactions, refer to transaction privacy in the non-functional
  requirements, the functional requirements are satisfied on a lower level than
  the Daml transaction.

  .. synchronizer-req:

  * **Synchronization:** The synchronizer must facilitate the synchronization of the
    shared ledger among participants by establishing a total order of
    transactions.

  .. _transparency-synchronizer-req:

  * **Transparency:** The synchronizer must inform the designated participants of
    changes to the shared ledger in a timely manner.

  .. _finality-synchronizer-req:

  * **Finality:** The synchronizer must facilitate the synchronization of the shared
    ledger in an append-only fashion.

  .. _unnecessary-rejects-synchronizer-req:

  * **No unnecessary rejections:** The synchronizer should minimize unnecessary
    rejections of valid transactions.

  .. _seek-support-synchronizer-req:

  * **Seek support for notifications:** The synchronizer must facilitate offset-based
    access to the notifications of the shared ledger.


  Non-Functional Requirements
  ---------------------------

  Reliability
  ^^^^^^^^^^^

  .. _fail-over-synchronizer-req:

  * **Seamless fail-over for synchronizer entities:** All synchronizer entities must be able
    to tolerate crash faults up to a certain failure rate, e.g., one sequencer node
    out of three can fail without interruption.

  .. _resilience-synchronizer-req:

  * **Resilience to faulty synchronizer behavior:** The synchronizer must be able to detect
    and recover from failures of the synchronizer entities, such as performing a
    fail-over on crash failures or retrying operations on transient failures if
    possible. The synchronizer should tolerate byzantine failures of the synchronizer
    entities.

  .. _backups-synchronizer-req:

  * **Backups:** The state of the synchronizer entities must be backed up such that
    in case of disaster recovery, only a minimal amount of data is lost.

  .. _disaster-recovery-synchronizer-req:

  * **Site-wide disaster recovery:** In case of a failure of a data-center hosting
    a synchronizer, the system must be able to fail over to another data center and
    recover operations.

  .. _resilience-participants-synchronizer-req:

  * **Resilience to erroneous behavior:** The synchronizer must be resilient to
    erroneous behavior from the participants interacting with it.

  Scalability
  ^^^^^^^^^^^

  .. _horizontal-scalability-synchronizer-req:

  * **Horizontal scalability:** The parallelizable synchronizer entities and their
    sub-components must be able to horizontally scale.

  .. _large-tx-synchronizer-req:

  * **Large transaction support:** The synchronizer entities must be able to cope with
    large transactions and their resulting large payloads.
    
  Security
  ^^^^^^^^

  .. _compromise-recovery-synchronizer-req:

  * **Synchronizer entity compromise recovery:** In case of a compromise of a synchronizer
    entity, the synchronizer must provide procedures to mitigate the impact of the
    compromise and allow to restore operations.

  .. _standard-crypto-synchronizer-req:

  * **Standards compliant cryptography:** All used cryptographic primitives and
    their configurations must comply with approved standards and be based on
    existing and audited implementations.

  .. _authnz-synchronizer-req:

  * **Authentication and authorization:** The participants interacting with the
    synchronizer as well as the synchronizer entities internal to the synchronizer must authenticate
    themselves and have their appropriate permissions enforced.

  .. _secure-channel-synchronizer-req:

  * **Secure channel (TLS):** All communication channels between the participants
    and the synchronizer as well as between the synchronizer entities themselves have to
    support a secure channel option using TLS, optionally with client
    certificate-based mutual authentication.

  .. _distributed-trust-synchronizer-req:

  * **Distributed Trust:** The synchronizer should be able to be operated by a
    consortium to distribute the trust of the participants in the synchronizer
    among many organizations.

  .. _transaction-privacy-synchronizer-req:

  * **Transaction Metadata Privacy:** The synchronizer entities must never learn the
    content of the transactions. The synchronizer entities should learn a limited amount
    of transaction metadata, such as structural properties of a transaction and
    involved stakeholders.

    Manageability
    ^^^^^^^^^^^^^

  .. _garbage-collection-synchronizer-req:

  * **Garbage collection:** The synchronizer entities must provide ways to minimize the
    amount of data kept on hot storage. In particular, data that is only required
    for auditability can move to cold storage or data that has been processed and
    stored by the participants could be removed after a specific retention period.

  .. _upgradeability-synchronizer-req:

  * **Upgradeability:** The synchronizer as a whole or individual synchronizer entities must
    be able to upgrade with minimal downtime.

  .. _semantic-versioning-synchronizer-req:

  * **Semantic versioning:** The interfaces, protocols, and persistent data
    schemas of the synchronizer entities must be versioned according to semantic
    versioning guidelines.

  .. _version-handshake-synchronizer-req:

  * **Synchronizer-approved protocol versions:** The synchronizer must offer and verify the
    supported versions for the participants. The synchronizer must further ensure
    that the synchronizer entities operate on compatible versions.

  .. _reuse-off-the-shelf-synchronizer-req:

  * **Reuse off-the-shelf solutions:** The synchronizer entities should use
    off-the-shelf solutions for persistence, API specification, logging, and
    metrics.

  .. _metrics-synchronizer-req:

  * **Metrics on communication and processing:** The synchronizer entities must expose
    metrics on communication and processing to facilitate operations and trouble
    shooting.

  .. _health-monitoring-synchronizer-req:

  * **Component health monitoring:** The synchronizer entities must expose a health
    endpoint for monitoring.


  Synchronizer-Internal Components
  ================================

  The following diagram shows the architecture and components of a Canton synchronizer
  as well as how a participant node interacts with the synchronizer.

  .. https://lucid.app/lucidchart/55638ee7-4fc8-46f2-af4f-a4752ad708d2/edit?invitationId=inv_6666f0bc-caaf-4065-9867-8e0348b63bca
  .. figure:: ./images/synchronizers/synchronizer-arch.svg
     :align: center
     :width: 80%

  The synchronizer consists of the following components:

  * **Synchronizer Service:** The first point of contact for a participant node when
    connecting to a synchronizer. The participant performs a version handshake with the
    synchronizer service and discovers the available other services, such as the
    sequencer. If the synchronizer requires a service agreement to be accepted by
    connecting participants, the synchronizer service provides the agreement.

  * **Synchronizer Topology Service:** The synchronizer topology service is responsible for
    all topology management operations on a synchronizer. The service provides the
    essential topology state to a new participant node, that is, the set of keys for
    the synchronizer entities to bootstrap the participant node. Furthermore,
    participant nodes can upload their own topology transactions to the synchronizer
    topology service, which inspects and possibly approves and publishes those
    topology transactions on the synchronizer via the sequencer.

  * **Sequencer Authentication Service:** A node can authenticate itself to the
    sequencer service either using a client certificate or using an authentication
    token. The sequencer authentication service issues such authentication tokens
    after performing a challenge-response protocol with the node. The node has to
    sign the challenge with its private key corresponding to a public key that
    has been approved and published by the synchronizer identity service.

  * **Sequencer Service:** The sequencer service establishes the total order of
    messages, including transactions, within a synchronizer. The service implements a
    total-order multicast, i.e., the sender of a message indicates the set of
    recipients to which the message is delivered. The order is established based
    on a unique timestamp assigned by the sequencer to each message.

  * **Sequencer Manager:** The sequencer manager is responsible for initializing
    the sequencer service.

  * **Mediator:** The mediator participates in the Canton transaction protocol and
    acts as the transaction commit coordinator to register new transaction
    requests and finalizes those requests by collecting transaction confirmations.
    The mediator provides privacy among the set of transaction stakeholders as
    the stakeholders do not communicate directly but always via the mediator.

  The synchronizer operator is responsible for operating the synchronizer infrastructure and
  (optionally) also verifies and approves topology transactions, in particular to
  admit new participant nodes to a synchronizer. The operator can either be a single
  entity managing the entire synchronizer or a consortium of operators, refer to the
  distributed trust security requirement.

  Drivers
  =======

  Based on the set of synchronizer internal components, a driver implements
  one or more components based on a particular technology. The prime component is
  the sequencer service and its ordering functionality, with implementations
  ranging from a relational database to a distributed blockchain. Components can
  be shared among integrations, for example, a mediator implemented on a
  relational database can be used together with a blockchain-based sequencer.

