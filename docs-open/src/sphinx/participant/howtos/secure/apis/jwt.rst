..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _jwt-authnz-api-configuration:

Configure API Authentication and Authorization with JWT
=======================================================

.. _ledger-api-jwt-configuration:

Configure authorization service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Ledger API supports `JWT <https://jwt.io/>`_ based authorization checks as described in the
:externalref:`Authorization documentation <authorization>`.

In order to enable JWT authorization checks, your safe configuration options are:

.. literalinclude:: CANTON/community/app/src/pack/config/jwt/certificate.conf

- ``jwt-rs-256-crt``.
  The participant will expect all tokens to be signed with RS256 (RSA Signature with SHA-256) with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

- ``jwt-es-256-crt``.
  The participant will expect all tokens to be signed with ES256 (ECDSA using P-256 and SHA-256) with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

- ``jwt-es-512-crt``.
  The participant will expect all tokens to be signed with ES512 (ECDSA using P-521 and SHA-512) with the public key loaded from the given X.509 certificate file.
  Both PEM-encoded certificates (text files starting with ``-----BEGIN CERTIFICATE-----``)
  and DER-encoded certificates (binary files) are supported.

- ``jwt-jwks``.
  Instead of specifying the path to a certificate, you can specify a`JWKS <https://tools.ietf.org/html/rfc7517>`__ URL.
  In that case, the Participant Node expects all tokens to be signed with RS256, ES256, or ES512 with the public key loaded
  from the given JWKS URL.

.. literalinclude:: CANTON/community/app/src/pack/config/jwt/jwks.conf

.. warning::

  For testing purposes only, you can also specify a shared secret. In
  that case, the Participant Node expects all tokens to be signed with
  HMAC256 with the given plaintext secret. This is not considered safe
  for production.

.. literalinclude:: CANTON/community/app/src/pack/config/jwt/unsafe-hmac256.conf

.. note:: To prevent man-in-the-middle attacks, it is highly recommended to use
          TLS with server authentication as described in :ref:`tls-configuration` for
          any request sent to the Ledger API in production.

Note that you can define multiple authorization plugins. If more than one is defined, the system will use the claim of the
first auth plugin that does not return Unauthorized.

If no authorization plugins are defined, the system uses a default (wildcard) authorization method.

.. _jwt-leeway-configuration:

Configure leeway parameters for JWT authorization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can define leeway parameters for authorization using JWT tokens.
An authorization that fails due to clock skew between the signing and the verification of the tokens can be eased by
specifying a leeway window in which the token should still be considered valid.
Leeway can be defined either specifically for the **Expiration Time ("exp")**, **Not Before ("nbf")** and
**Issued At ("iat")** claims of the token or by a default value for all three. The values defining the
leeway for each of the three specific fields override the default value if present. The leeway parameters should be
given in seconds and can be defined as in the example configuration below:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/leeway-parameters.conf

Configure the target audience for JWT authorization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default audience (``aud`` field in the audience-based token) for authenticating on the Ledger API using JWT is
``https://daml.com/participant/jwt/aud/participant/${participantId}``. Other audiences can be configured explicitly
using the custom target audience configuration option:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/ledger-api-target-audience.conf

Configure the target scope for JWT authorization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default scope (``scope`` field in the scope-based access token) for authenticating on the Ledger API using JWT is ``daml_ledger_api``.

Other scopes can be configured explicitly using the custom target scope configuration option:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/ledger-api-target-scope.conf

Target scope can be any case-sensitive string containing alphanumeric characters, hyphens, slashes, colons and underscores.
Either the ``target-scope`` or the ``target-audience`` parameter can be configured individually, but not both at the same time.

Configure authorization service with user list
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Authorization service with user list is a form of JWT Authorization that can be configured on the Admin API exposed by the Participant,
the Sequencer, and the Mediator Nodes as well as being available on the Ledger API of the Participant Node.

You can specify which users should be allowed in and which gRPC services should be accessible to them.
An example configuration for both the Ledger and Admin APIs looks like this:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/ledger-api-user-list.conf

Unlike the Ledger API, the Admin API doesn't require the users appearing in the sub-claims of the JWT tokens to be present in the
Participant’s user database. The user in the authorization service config\
can be an arbitrary choice of the Participant Node’s operator. This user also needs to be configured in the associated
IDP system issuing the JWT tokens.

The configuration can contain a definition of either the target audience or the target scope depending on the specific
preference of the client organization. If none is given, the JWT tokens minted by the IDP must specify ``daml_ledger_api``
as their scope claim.

Independent of the specific service that the operator wants to expose, it is a good practice to also give access rights
to the ``ServerReflection`` service. Some tools such as ``grpcurl`` or ``postman`` need to hit that service to construct
their requests.

Configure privileged tokens
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Privileged tokens are another form of JWT Authorization that can be configured on the Admin API exposed by the Participant, Sequencer, and Mediator Nodes, as well as being available on the Ledger API of the Participant Node.
The configuration follows the same pattern as normal user tokens, but it contains an additional key called ``privileged``
set to true.

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/ledger-api-privileged.conf

You can determine if the use of privileged tokens should result in granting of the ``participant_admin``
or the ``wildcard`` access levels by adding a definition of the ``access-level`` key and setting it to
``Admin`` or ``Wildcard`` respectively. The ``Admin`` is the default.

Configure remote console for JWT authorization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the authorization is configured, the remote console will only work against a participant when a JWT token is made
available to the console in the config:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/ledger-api-remote-console.conf
