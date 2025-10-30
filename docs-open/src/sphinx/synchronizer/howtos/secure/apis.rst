..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Link to participant for API security for Admin API for mediator and sequencer.
    Link to participant for TLS config on Public API.
    Cover authentication token manager config for the Public API auth.
    Link to explanation on sequencer Public API authentication.
    Highlight no mutual TLS option on the Public API.

.. _secure-synchronizer-apis:

Secure Synchronizer APIs
========================

Sequencer Public API
--------------------

Public API
~~~~~~~~~~
The synchronizer configuration requires the same configuration of the ``Admin API`` as the participant.
Next to the ``Admin API``, we need to configure the ``Public API``, which is the API where
all participants connect.

TLS Encryption
^^^^^^^^^^^^^^
As with the Admin API, network traffic can (and should) be encrypted using TLS. This is particularly
crucial for the Public API.

An example configuration section which enables TLS encryption and server-side TLS authentication is given by:

.. literalinclude:: CANTON/community/integration-testing/src/main/resources/include/synchronizer2.conf
   :start-after: architecture-handbook-entry-begin: SynchronizerPublicApi
   :end-before: architecture-handbook-entry-end: SynchronizerPublicApi

If TLS is used on the server side with a self-signed certificate, we need to pass the
certificate chain during the connect call of the participant. Otherwise, the default root
certificates of the Java runtime will be used. An example would be:

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/enterprise/app/src/test/scala/com/digitalasset/canton/integration/tests/MultiSynchronizerIntegrationTests.scala
    start-after: architecture-handbook-entry-begin: TlsConnect
    end-before: architecture-handbook-entry-end: TlsConnect
    dedent:

Server Authentication
^^^^^^^^^^^^^^^^^^^^^

Canton has two ways to perform server authentication to protect from man-in-the-middle attacks: TLS and the synchronizer id.

If TLS is used on the Public API as described above, TLS also takes care of server authentication. This is one of the
core functions of TLS.

Server authentication can also be performed by the synchronizer operator passing their synchronizer identity to the
participant node operator, and checking that the identity matches that reported by the synchronizer to the participant
node. Like all nodes, the synchronizer has an identity that corresponds to the fingerprint of its namespace root key.
It reports its identity to connecting participant nodes and signs all its messages with keys authorized by that
namespace root key on the topology ledger. Assuming no key compromises, this gives participants a guarantee that the
reported identity is authentic. The synchronizer id of the sole connected synchronizer can be read out using console
commands like:

.. code-block:: none

    participant1.synchronizers.list_connected.last.synchronizerId.filterString

Client Authentication
^^^^^^^^^^^^^^^^^^^^^

Unlike the ledger or Admin API, the Public API uses Canton's cryptography and topology state for client authentication
rather than mutual TLS (mTLS). Clients need to connect to the Public API in several steps:

#. The client calls the ``SequencerConnectService`` to align on Canton Protocol versions and obtain the synchronizer id.
#. During the first connection, the client registers by sending its minimal topology state (identity, key delegations,
   public keys) to the sequencer.
#. The client calls the ``SequencerAuthenticationService`` to authenticate using a challenge-response protocol and get
   an access token for the other sequencer services.
#. The client connects to the main ``SequencerService`` using the access token from 3.

The information the client provides in step 2 is verifiable since it is a certificate chain of keys. The synchronizer rejects
this if the namespace root key fingerprint included is not permissioned  (see :ref:`permissioned-synchronizer`) or if the
topology state provided is invalid.

During step 3, the client claims an identity, which is the fingerprint of a namespace root key. If that identity is
registered (as done in step 2), the sequencer responds with a challenge consisting of a nonce and all fingerprints of
signing keys authorized for that member as per the topology ledger.
If the challenge is met successfully by signing the nonce appropriately with a key matching one of the authorized
keys, the ``SequencerAuthenticationService`` responds with a time-limited token which can be used to authenticate more
cheaply on the other Public API services.

This authentication mechanism for the restricted services is built into the public sequencer API. You don't need to do
anything to set this up; it is enforced automatically and can't be turned off.

The expiration of the token generated in step 2 is valid for one hour by default.
The nodes automatically renew the token in the background before it expires. The lifetime of the
tokens and of the nonce can be reconfigured using

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/auth-token-config.conf

However, we suggest keeping the default values.

As mentioned above, an issued token allows the member that provides it during a call to authenticate on public sequencer API
services. Therefore, these tokens are sensitive information that must not be disclosed. If an operator suspects that
the authentication token for a member has been leaked or somehow compromised, they should use the ``logout`` console command to immediately revoke all valid tokens of
that member and close the sequencer connections. The legitimate member automatically reconnects and obtains
new tokens through the challenge-response protocol described above.

The command is slightly different depending on whether the member is a participant or a mediator, for example:

.. todo::
   Replace with references to the commands.
   `#22919 <https://github.com/DACH-NY/canton/issues/22919>`_

.. code-block:: text

   participant1.synchronizers.logout(mySynchronizerAlias)
   mediator1.sequencer_connections.logout()

