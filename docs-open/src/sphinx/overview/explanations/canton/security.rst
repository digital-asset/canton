..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _security_architecture:

Cryptographic keys in Canton
============================

This section is about cryptographic keys in Canton.
As a preliminary, it outlines general options for :ref:`storing secrets in Canton<storage-secrets>`.
Then, it gives an :ref:`overview of the various kinds of cryptographic keys<overview-crypto-keys>` and how Canton uses and stores them.


.. _storage-secrets:

Storage of secrets
-----------------------

This section outlines the general options for storing secrets in Canton.

Plaintext storage options
"""""""""""""""""""""""""

For convenience, Canton offers the following options without encryption:

- **Database:** A Canton node stores a secret in its database in plaintext.

- **File:** A Canton node stores a secret in a file in plaintext.

Options without persistence
"""""""""""""""""""""""""""

For better protection of secrets, Canton also offers options without persistence:

.. _storage-option-offline:

- **Offline:** For some secrets, Canton allows for not storing them at all.
  Hence, usages of the secrets (such as signing and decryption operations) need to happen outside of Canton.

- **In memory:** Canton stores a secret in memory in plaintext.
  It does not store the secret in a file nor in the database.

Options with a key management service
"""""""""""""""""""""""""""""""""""""

For better protection of secrets that Canton needs to persist, Canton allows for using a :externalref:`key management service (KMS)<kms>`:

- **KMS envelope encryption:** Canton stores a secret in an encrypted manner in the database.
  Canton keeps the secret as plaintext in memory.
  When Canton needs to decrypt the secret, it sends its ciphertext to a KMS,
  the KMS decrypts the secret and sends the plaintext of the secret back to Canton.

- **Full KMS:** A KMS generates and stores a secret, typically a private signing or encryption key.
  The KMS does not expose the secret to anybody.
  When Canton needs to sign some data, it sends the data to the KMS, the KMS produces the digital signature, and
  sends the signature back to Canton.
  Similarly, when Canton needs to decrypt some data, it sends the ciphertext to the KMS, the KMS decrypts the ciphertext,
  and sends the plaintext back to Canton.

.. _overview_session_signing_keys:

KMS with session signing keys
"""""""""""""""""""""""""""""

As the "full KMS" option comes with substantial latency, Canton offers an alternative, which must be
:externalref:`explicitly enabled <enable_session_signing_keys>`, that provides lower signing latency at the cost of
slightly weaker protection of private signing keys. In this mode, a node stores its long-term private signing key in
a KMS, but, for signing, it uses a session signing key instead of the long-term key. To do that, alongside with the
signature, it also ships (1) the public key corresponding to the private session signing key and (2) a certificate
demonstrating that the session key is a legitimate replacement of the long-term key during a defined period of time.
The long-term key needs to sign this certificate.

This approach limits the overhead of KMS operations, as the node accesses the KMS only when rolling the session key
(as opposed to on every signing operation).

The node keeps the session signing key in memory in plaintext for a configurable lifetime.
The node does not store session signing keys on disk nor in the database.
To limit the impact of a potential key compromise, a node does not accept signatures from a session signing key
outside of the time period defined in the certificate.
Session signing keys are only used to sign :externalref:`protocol messages <signing-key-usage-restrictions>`.

.. _parametrization_session_signing_keys:

Configurable parameters
+++++++++++++++++++++++

Session signing keys have a validity period associated with them every time they are created, and they are only valid
during that period. This period can be adapted through Canton's configuration files. This section lists the
configurable parameters, what they control, and what to keep in mind when modifying them. Throughout this section
``ts`` denotes the timestamp at which we are signing.

- **enabled**

  - Enables the usage of session signing keys in the protocol. This option can only be used when a KMS is deployed and the private keys are stored within it.

- **keyValidityDuration**

  - Specifies the validity duration for each session signing key. Its lifespan **must** satisfy: ``keyValidityDuration > 2 * cutoffDuration``.

  - The validity duration should also not be too short, so that a session signing key can cover the lifespan of a message (i.e., its maximum sequencing time) and be reused across multiple submission requests.

  - Recommended (optional) constraints. If these are not satisfied, the operator should be aware that some messages may need to be signed with the long-term key, which may lead to increased load on the KMS and more frequent calls to the KMS:

     1. ``keyValidityDuration > defaultMaxSequencingTimeOffset``
     2. ``keyValidityDuration > setBalanceRequestSubmissionWindowSize``
     3. ``keyValidityDuration > confirmationResponseTimeout + mediatorReactionTimeout``

- **toleranceShiftDuration**

  - Shifts the validity interval from ``[ts, ts+keyValidityDuration]`` to ``[ts-toleranceShiftDuration, ts+keyValidityDuration-toleranceShiftDuration]``.

  - It **must** respect the following constraints:

     1. ``toleranceShiftDuration < keyValidityDuration - cutoffDuration``
     2. ``toleranceShiftDuration > cutoffDuration``

  - We use this tolerance to minimize the number of intervals (i.e., session signing keys) created. For example, without a shift, a sequence of timestamps ``ts, ts-1us, ts-2us, ts-3us`` would generate multiple keys. The value can be adjusted depending on whether timestamps are mostly increasing or decreasing. A shift of around 50% of the ``keyValidityDuration`` is generally recommended.

- **cutoffDuration**

  - Measures the tolerable clock skew and should be longer than ``ledgerRecordTimeTolerance``. This ensures that submissions do not fail verification due to clock skew.

  - The node uses an existing session signing key only if the key’s validity period fully covers the required time range, with a buffer/margin applied on both sides.

- **keyEvictionPeriod**

  - Defines how long the private session signing key remains in memory.

  - **Must be longer** than ``keyValidityDuration``.

- **signingAlgorithmSpec**

  - Defines the signing algorithm for session signing keys. Defaults to ``Ed25519``.

- **signingKeySpec**

  - Defines the key scheme for session signing keys. Defaults to ``EcCurve25519``.

  - Both algorithm and key scheme must be supported and allowed by the node.

.. _session-encryption-keys:

Session encryption keys
"""""""""""""""""""""""

To reduce the load caused by asymmetric encryption and decryption operations,
Canton uses symmetric encryption instead.
Thereby, Canton greatly reduces its use of asymmetric encryption.

Every node has a long-term asymmetric encryption key, which it stores in its database or in a KMS.
Additionally, every node produces session symmetric encryption keys and uses them to encrypt data it needs to protect.
Alongside with a ciphertext, a node also ships the corresponding session encryption key asymmetrically encrypted with the long-term encryption key of the recipient.

In general, the data has a much bigger size than a session key.
Moreover, a node usually needs to send the data to various recipients.
Under these assumptions, the use of session encryption keys reduces the load of encryption and decryption operations for various reasons:

- Canton needs to encrypt the data only once (as opposed to once per recipient) and
  it needs to send only one ciphertext of the data.
- Canton uses the cheaper symmetric encryption for the bigger data
  and the more expensive asymmetric encryption for the smaller session key.
- A node may use the same session key to encrypt several pieces of data.
  In that case, the recipient needs to decrypt the session key only when it receives it for the first time.

Canton nodes keep session encryption keys in memory in plaintext for a configurable lifetime and independently of a session.
They do not store the plaintexts of session encryption keys on disk nor in the database.
They may store session keys in an encrypted fashion in their databases.
To limit the impact of a potential key compromise, a node does not use a session encryption key after its configurable lifetime has elapsed.
A node reuses the same session encryption key for encrypting data d1 and data d2
only if the intended recipients of d1 coincide with the intended recipients of d2 and
their public encryption keys have not been rolled in between.

.. _overview-crypto-keys:

Overview of cryptographic keys in Canton
----------------------------------------

This section provides an overview of how Canton makes use of cryptographic keys.

TLS keys
""""""""

Canton nodes provide various gRPC and HTTP apis that allow clients to interact with the node.
Examples include:

- ledger apis of Participant Nodes for submitting ledger commands and receiving ledger updates,
- admin apis of all kinds of nodes for administrative purposes,
- apis of Sequencers for multicasting messages,
- ordering apis for internal communication between Sequencers,
- health apis.

Users can configure the apis of a node to use server side TLS and, in some cases, also to use mutual TLS.
Canton reads the keys that TLS uses from one or several files at startup.
These files contain the TLS keys in plaintext.


Namespace signing keys
""""""""""""""""""""""

Every :ref:`topology namespace<topology-namespaces>` has a public signing key, called the `namespace root key`.
The namespace is the hash of its root key.
Operators use the private key corresponding to the root key to sign and thereby authorize topology transactions for the namespace.
A namespace can have :ref:`further signing keys<topology-delegation>` and operators can use them as well to authorize topology transactions for the namespace.

Examples of topology transactions that a namespace key (root or not) can authorize include:

- :ref:`NamespaceDelegation <topology-delegation>`: authorize further keys to authorize topology transactions related to the namespace,
- :ref:`OwnerToKeyMapping <topology-cryptographic-keys>`: associate a key with a Canton node,
- :ref:`PartyToParticipant <topology-parties>`: associate a party with a participant.

Canton supports the following :ref:`options<storage-secrets>` for storing private namespace keys:

- database (the default),
- :externalref:`offline<namespace-root-key-offline>`,
- KMS envelope encryption,
- full KMS.

Node signing keys
"""""""""""""""""

A node has one or several signing keys that serve the following purposes:

- Sequencer client authentication:
  When interacting with the public api of a Sequencer, clients need to provide a valid authentication token.
  A client obtains its authentication token in a challenge-response protocol.
  During the protocol, the client signs a nonce generated by the Sequencer.

  Additionally, a request to the Sequencer needs to include a signature from its sender if the request may change the Sequencer's state.

- Sequencer server authentication:
  The Sequencer signs every event that it emits to a client.

- When processing Ledger API commands, Participant Nodes use their signing keys to sign and therefore authenticate
  messages sent as part of the transaction and reassignment protocols.
  This includes both processing of Daml transactions and reassignments.

- Sequencers exchange messages as part of their ordering protocol.
  They use their signing keys for authenticating the senders of such messages.

- Participant Nodes use their signing keys to sign ACS commitments.

Canton supports the following :ref:`options<storage-secrets>` for storing node signing keys:

- database (the default),
- all options storing the keys in a KMS.
  Exception: Canton does not support usage of KMS with session signing keys for sequencer client authentication.


External party signing keys
"""""""""""""""""""""""""""

By default, Participant Nodes sign and thereby authorize the execution of Daml transactions.
Alternatively, the submitting party (referenced as :ref:`external party<overview_canton_external_parties>`) may
directly sign and authorize the Daml Transaction that it intends to add to the ledger.
That is, the submitting party signs instead of the submitting Participant Node.
For this purpose, external parties have their own signing keys.

The recommended option for storing signing keys of external parties is :ref:`"offline"<storage-option-offline>`.
Other options (for example database) may be technically possible, but they are not recommended.


Encryption keys
"""""""""""""""

When a Participant Node proposes a transaction, it decomposes the transaction into so called :brokenref:`transaction views<canton-overview-confirmation-request>`.
It sends every transaction view to exactly those Participant Nodes that may see its contents.
The decomposition of a transaction into views is crucial to achieve sub-transaction privacy.

Participant Nodes have one or several asymmetric encryption keys.
When a Participant Node sends a transaction view to other Participant Nodes,
it does so in an encrypted fashion.
This is important so that Sequencer Nodes do not see the contents of transaction views.
Participant Nodes do not directly encrypt transaction views using their asymmetric encryption keys,
but instead they apply :ref:`session encryption keys<session-encryption-keys>` for better performance.

Nodes other than Participant Nodes (for example, Sequencer Nodes) can also have encryption keys.
However, Canton does currently not use encryption keys of such nodes.


Tokens for authentication and authorization
"""""""""""""""""""""""""""""""""""""""""""

At some APIs, Canton Nodes require that clients include a token into every request for the sake of authentication or authorization.
Canton stores such tokens in memory.
A Canton node rejects a request if the lifetime of its token has elapsed or
if the token does not prove that the client has permissions to send the request.

It is crucial to enable TLS on APIs that receive tokens,
as an attacker could otherwise learn authentication tokens by inspecting network traffic.

Canton makes use of tokens on the following APIs:

- The public API of Sequencer Nodes mandates the use of tokens for every request.
- Operators can configure Participant Nodes to :externalref:`enable authorization at the Ledger API<ledger-api-jwt-configuration>`.
  If enabled, clients of a Ledger API need to include a `JWT <https://jwt.io>`_ token into every request.
