..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _tls-configuration:

TLS API Configuration
=====================

Both the gRPC Ledger API and the Admin API support the same TLS capabilities and can be configured using
identical directives. TLS provides end-to-end channel encryption between the server and the client,
and—depending on the configuration—can enforce either server-only or mutual authentication.

The example configurations presented in this section are for the Ledger API, but they are the same for the Admin API.
All private keys mentioned in these how-tos must be in the PKCS#8 PEM format.

Enable TLS for server side authentication
-----------------------------------------

The following settings allow a connecting client to verify that it is communicating with the intended server.

.. literalinclude:: CANTON/community/integration-testing/src/main/resources/include/participant4.conf
   :start-after: architecture-handbook-entry-begin: ParticipantTLSApiServer
   :end-before: architecture-handbook-entry-end: ParticipantTLSApiServer

The ``trust-collection-file`` allows you to provide a file-based trust store. If omitted, the system
will default to the built-in ``JVM`` trust store.
The format is a collection of PEM certificates (in the right order or hierarchy), not a Java-based trust store.

Enable TLS for client side authentication
-----------------------------------------

Client-side authentication is disabled by default. To enable it, you must configure the ``client-auth.type`` to
``require``.

.. literalinclude:: CANTON/community/integration-testing/src/main/resources/include/participant4.conf
   :start-after: architecture-handbook-entry-begin: ParticipantTLSApiClient
   :end-before: architecture-handbook-entry-end: ParticipantTLSApiClient

This new ``client-auth`` setting enables client authentication, which means that the client needs to
present a valid certificate (and have the corresponding private key).

The ``trust-collection-file`` must contain all client certificates
(or parent certificates that were used to sign the client certificate) that are trusted to use
the API. A certificate is valid if it has been signed by a key in the trust store.

Restrict TLS ciphers and protocols
----------------------------------

Assuming the server-side configuration has been completed as described in the howto above,
the following settings can be used to restrict the allowed TLS ciphers and protocol versions.
By default, Canton uses the JVM default values, but you can override them using the
variables ``ciphers`` and ``minimum-server-protocol-version``.

.. literalinclude:: CANTON/community/integration-testing/src/main/resources/include/participant4.conf
   :start-after: architecture-handbook-entry-begin: ParticipantTLSApiRestrict
   :end-before: architecture-handbook-entry-end: ParticipantTLSApiRestrict
