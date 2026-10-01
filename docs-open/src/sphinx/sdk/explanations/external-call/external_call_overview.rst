..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _external_call_overview:

==============
External Calls
==============

.. warning::
   External calls are an early-access feature. They are currently available only on synchronizers
   running a protocol version that supports them and require the Daml package to target the
   corresponding LF version. Early-access features are not covered by production support.

External calls let a Daml choice invoke a function on an *extension service*: a deterministic
service that the participant node operator configures and runs alongside the participant. The
call's result is recorded in the transaction, so every confirming participant can validate it
against its own extension service before approving the transaction. This extends the ledger model
with computations that are impractical to express in Daml (for example, verifying an exotic
signature scheme) while preserving the guarantee that all stakeholders agree on the result.

External call flow
==================

1. Execute the call at submission time
--------------------------------------

When a submitted command exercises a choice that uses ``externalCall``, the submitting participant
sends the call's configuration and input payloads to the extension service configured under the
requested extension identifier and records the service's output. Interpretation fails if the
extension is not configured, the service cannot be reached after the configured retries, or the
service reports an error.

2. Record the result in the transaction
---------------------------------------

The recorded result — extension identifier, function identifier, configuration, input, and
output — travels with the transaction on the exercise node that made the call. Parties that see
the exercise node see the recorded result. For externally signed transactions, the recorded
results are part of the prepared transaction and are covered by the signed transaction hash
(hashing scheme V4).

3. Validate the result at confirmation time
-------------------------------------------

Confirming participants validate recorded results twice over:

- **Consistency:** all visible occurrences of the same call (same extension, function,
  configuration, and input) must record the same output. A disagreement raises a security alarm
  on every participant that receives the views and rejects the affected views.
- **Re-validation:** participants hosting a confirming party responsible for a recorded call
  re-execute the call against *their own* extension service and compare the output with the
  recorded one. A mismatch is treated the same way as a recorded disagreement. A participant
  that cannot re-validate (for example, because no extension service is configured) abstains
  from confirming instead of approving.

4. Use the result
-----------------

The choice body receives the service output as the result of the ``externalCall`` update and can
use it like any other value; the transaction commits only if validation succeeds on the
confirming participants.

Writing Daml code with external calls
=====================================

The function is provided by the Daml standard library module ``DA.ExternalCall``:

.. code-block:: daml

   import DA.ExternalCall

   externalCall : Text -> Text -> Text -> Text -> Update Text

``externalCall extensionId functionId config input`` requests a call to function ``functionId``
of the extension configured on the participant under ``extensionId``. The ``config`` argument
is the extension configuration hash and ``input`` is the call input; both are hex-encoded byte
strings, and the empty string represents zero bytes.
Payloads are canonicalized to lowercase before execution, so two calls that differ only in hex
casing are the same call. The result is the service response, again as a hex-encoded byte
string. Malformed payloads, missing extension configuration, extension-service failures, and
invalid service output fail the update; runtime and service errors include the extension and
function context.

Operational and security assumptions
====================================

Determinism
-----------

.. important::
   Extension services **must be deterministic**: for a given extension, function, configuration,
   and input, every honest service instance must always return the same output. The protocol
   detects non-determinism — validation compares recorded and recomputed outputs — and treats it
   as suspicious behavior: the transaction is rejected and a security alarm is raised. A service
   that depends on wall-clock time, randomness, or mutable external state will make transactions
   fail non-deterministically.

Topology and connectivity
-------------------------

Which nodes need to reach an extension service:

- The **submitting participant** executes the call at submission time and therefore needs the
  extension configured and reachable.
- Every **confirming participant** that hosts a party responsible for checking a recorded call
  re-executes the call at validation time and therefore also needs the *same* extension (same
  identifier, semantically identical service) configured and reachable. A confirming participant
  without the extension abstains, which can prevent the transaction from being committed.
- Participants that only observe the transaction do not contact the service.

Operators of participants that confirm external-call workflows should treat the extension
service as part of the participant's availability domain: if the service is down, submissions
fail at the submitting participant, and validations abstain at the confirming participants.

Trust model
-----------

- A participant trusts **its own** extension service to produce correct, deterministic outputs.
  The service runs under the participant operator's control and is addressed over an
  operator-configured endpoint (with optional TLS and bearer-token authentication).
- A participant does **not** need to trust another participant's extension service, nor the
  submitter: recorded results are re-validated locally against the participant's own service
  before confirmation.
- The extension service sees the configuration and input payloads of every call routed to it,
  including at validation time for transactions submitted by others. Canton itself never writes
  payload contents to its logs or error messages — diagnostics carry payload sizes only — but
  the service must be trusted with the payload data it receives (including what it echoes into
  its own error responses).

Configuring an extension service
================================

Extension services are configured per participant under
``canton.participants.<participant>.parameters.engine.extensions``, keyed by the extension
identifier that Daml code passes to ``externalCall``:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/external-call.conf

Notable parameters (see the scaladoc of ``ExtensionServiceConfig`` for the full reference):

- ``address``, ``port``, ``version``: the endpoint of the extension service's HTTP API.
- ``tls``: enable TLS towards the service, with ``tls.trust-collection-file`` naming the trust
  anchors.
- ``auth``: optional bearer-token authentication (``type = bearer-token-file`` with
  ``token-file``); the token is never logged.
- ``connect-timeout``, ``request-timeout``, ``max-retries``, ``retry-initial-delay``,
  ``retry-max-delay``: connection handling; retries apply per logical call.
- ``max-response-body-bytes``: upper bound on accepted service responses.
- ``validate-on-startup``: probe the service's version endpoint when the participant starts.

Additional resources
====================

- :ref:`External signing hashing algorithm <hashing_scheme_version>` — hashing scheme V4 covers
  recorded external-call results in externally signed transactions.
- :ref:`Error codes reference <error_codes>` — the external-call validation error codes carry
  explanations and resolutions generated from the participant sources.
