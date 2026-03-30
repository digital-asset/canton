# External Call OAuth Specification

## Normative Conventions

The key words `MUST` and `MUST NOT` are normative.

## Scope

The participant `external_call` implementation MUST support OAuth 2.0 service-to-service
authentication using:

- `grant_type = client_credentials`
- client authentication by `private_key_jwt`
- bearer access tokens

The implementation MUST preserve the existing Daml-level business protocol. The implementation MUST
NOT change the Daml-visible inputs `(extensionId, functionId, configHash, input, mode)`. The
implementation MUST NOT change the successful business response body format. The implementation MUST
surface OAuth failures only through the existing external-call transport error boundary.

OAuth configuration MUST remain per extension.

The implementation MUST replace the existing static bearer-token fields `jwt` and `jwtFile`. The
implementation MUST NOT retain those fields as the long-term supported auth model.

The implementation MUST NOT introduce:

- a general auth-provider subsystem
- a general transport abstraction for participant extensions
- startup validation integration or participant startup gating as part of OAuth v1
- token-request `audience`
- sender-constrained tokens or mTLS-bound access tokens
- proactive or background token refresh
- hot reload of OAuth key material or trust material

## Determinism

For a fixed `(extensionId, functionId, configHash, input)`, successful business responses MUST be
identical in `submission` and `validation`.

The implementation MUST forward `mode` unchanged on the existing wire contract.

The implementation MUST treat the following as transport state only and MUST NOT use them as
business inputs:

- access-token claims
- client-assertion timestamps
- `jti`
- token expiry bookkeeping
- OAuth client identity

## Repository Boundary

The implementation MUST preserve the existing repository seam.

`community/participant/src/main/scala/com/digitalasset/canton/participant/ParticipantNode.scala`
MUST instantiate `ExtensionServiceManager` when `parameters.engine.extensions` is non-empty.

`community/participant/src/main/scala/com/digitalasset/canton/participant/extension/ExtensionServiceManager.scala`
MUST instantiate one `HttpExtensionServiceClient` per configured extension and MUST route calls by
extension ID.

`community/participant/src/main/scala/com/digitalasset/canton/participant/extension/ExtensionServiceExternalCallHandler.scala`
MUST remain a thin mapper from extension errors to `ExternalCallError` and MUST expose only:

- `statusCode`
- `message`
- `requestId`

`community/participant/src/main/scala/com/digitalasset/canton/participant/extension/HttpExtensionServiceClient.scala`
MUST own:

- request orchestration
- token acquisition
- token caching
- the outer retry loop
- the OAuth-specific `401` refresh-and-replay
- error mapping

The implementation MUST NOT move those responsibilities into `ExtensionServiceManager`.

## Resource-Server Protocol

The implementation MUST preserve the existing resource-server protocol:

- HTTP method MUST remain `POST`
- HTTP path MUST remain `/api/v1/external-call`
- `Content-Type` MUST remain `application/octet-stream`
- `X-Daml-External-Function-Id` MUST carry `functionId`
- `X-Daml-External-Config-Hash` MUST carry `configHash`
- `X-Daml-External-Mode` MUST carry `mode`
- the configured request ID header MUST carry the participant-generated request ID
- the request body MUST remain the existing business payload
- the successful response body MUST remain the existing business payload

When OAuth is enabled, the implementation MUST add `Authorization: Bearer <token>` to the resource
request. When OAuth is disabled, the implementation MUST NOT add that header.

## HTTP Client Ownership

Each `HttpExtensionServiceClient` MUST own the outbound HTTP client state for its extension.

When `auth.type = none`, the extension client MUST create exactly one internal Java `HttpClient`
dedicated to resource-server calls.

When `auth.type = oauth`, the extension client MUST create exactly two internal Java `HttpClient`
instances:

- one dedicated to resource-server calls
- one dedicated to token-endpoint calls

The implementation MUST NOT require cross-extension HTTP client sharing.

## Retry and Deadline Model

`HttpExtensionServiceClient.callWithRetry` MUST remain the only business-request retry loop.

The outer retry classification MUST remain:

- terminal: `400`, `401`, `403`, `404`
- retryable: `408`, `429`, `500`, `502`, `503`, `504`

For one external-call operation, the implementation MUST compute one absolute deadline from
`max-total-timeout` before the first outer attempt.

The implementation MUST NOT start token acquisition or a resource request when the remaining budget
is non-positive.

`maxRetries` MUST count only outer retries.

One outer attempt MUST execute as follows:

1. The implementation MUST compute the remaining budget from the fixed absolute deadline.
2. The implementation MUST resolve auth:
   - `auth.type = none`: the implementation MUST skip auth work.
   - `auth.type = oauth`: the implementation MUST obtain a valid access token against the same
     remaining budget.
3. The implementation MUST build the resource request with the preserved resource-server protocol.
4. The implementation MUST apply request timeout as
   `min(configured request-timeout, remaining budget)`.
5. The implementation MUST send the resource request.
6. If the resource response is not `401`, that response MUST be the outcome of the outer attempt.
7. If the resource response is `401` and OAuth is enabled:
   - the implementation MUST invalidate the cached token only if it is the same token that was sent
     on the rejected request
   - the implementation MUST obtain a fresh token against the same outer deadline
   - the implementation MUST replay the resource request exactly once with the fresh token
8. The implementation MUST feed the replay result, or the original non-`401` result, back into the
   existing outer retry loop.

The auth-local replay MUST NOT consume a `maxRetries` slot.

`401` MUST be the only resource response that triggers OAuth-specific recovery after a resource
request has been sent.

After the auth-local replay, the implementation MUST resume normal status handling:

- `200` MUST succeed
- `400`, `401`, `403`, and `404` MUST be terminal
- `408`, `429`, `500`, `502`, `503`, and `504` MUST remain retryable through the outer loop

`connect-timeout` MUST be a fixed per-extension client setting. OAuth v1 MUST clamp per-request
`request-timeout` to the remaining total budget. OAuth v1 MUST NOT require dynamic per-attempt
connect-timeout clamping.

## Token Cache

OAuth token caching MUST be on-demand.

The implementation MUST satisfy all of the following:

- A cached access token MUST be reused while the client considers it unexpired.
- An expired cached token MUST be replaced on the next business request that requires OAuth.
- A cached token rejected by the resource server with `401` MUST be invalidated for one refresh-and-
  replay attempt.
- The implementation MUST NOT perform proactive refresh.
- The implementation MUST NOT run a background refresh task.

Concurrent cache misses for the same extension MUST be serialized so that at most one token
acquisition is in flight per extension at a time.

When one request is already refreshing the token for an extension, other concurrent requests for the
same extension MUST wait for that refresh result instead of starting a second token acquisition.

## Token Request

The token request MUST use OAuth 2.0 client credentials with `private_key_jwt`.

The token request MUST use HTTP `POST`.

The token request MUST use `Content-Type: application/x-www-form-urlencoded`.

The implementation MUST generate a participant request ID for each token-endpoint HTTP interaction.

The implementation MUST attach that request ID using the configured `request-id-header`.

The token request MUST include:

- `grant_type = client_credentials`
- `client_assertion_type = urn:ietf:params:oauth:client-assertion-type:jwt-bearer`
- `client_assertion = <signed JWT>`

If `scope` is configured, the token request MUST include `scope`.

If `scope` is not configured, the token request MUST omit `scope`.

The token request MUST NOT include token-request `audience`.

The implementation MUST treat access tokens as opaque bearer tokens. The implementation MUST NOT
parse or locally verify access-token claims.

The token response MUST contain:

- `access_token`
- `token_type`
- `expires_in`

The implementation MUST reject the token response unless `token_type` equals `Bearer`,
case-insensitively.

The implementation MUST use `expires_in` to compute the local expiry instant used for cache reuse.

A token response with missing or malformed required fields MUST be treated as malformed.

## Client Assertion

The `private_key_jwt` client assertion MUST use:

- signing algorithm `RS256`
- `iss = client-id`
- `sub = client-id`
- `aud = <token-endpoint URI>`
- `iat = now`
- `exp = now + 30s`
- `jti = <fresh random identifier>`

If `key-id` is configured, the implementation MUST include `kid`.

If `key-id` is not configured, the implementation MUST omit `kid`.

Each assertion MUST be one-use only.

The implementation MUST NOT log or persist client assertions.

The supported signing-key format for v1 MUST be RSA DER / PKCS#8.

## Key and Trust Material

OAuth key and trust material MUST be initialized on demand.

The implementation MUST NOT require signing-key, trust material, or OAuth-specific HTTP client state
to load successfully during `HttpExtensionServiceClient` construction or participant startup.

Before the first outbound HTTP interaction that requires OAuth material, the implementation MUST
ensure that the required signing key, trust material, and OAuth-specific HTTP client state are
initialized.

After the first successful initialization, the implementation MUST reuse the initialized material
and client state for the lifetime of the `HttpExtensionServiceClient` and MUST NOT re-read that
material on each token request or resource request.

The implementation MUST assume that key rotation and trust-material rotation take effect only after
participant restart.

## Token-Endpoint Failures

Token-endpoint failures MUST consume the same outer retry budget as resource-server failures.

OAuth v1 MUST NOT define a second retry policy.

For any token-endpoint HTTP response, the implementation MUST preserve the exact HTTP status code in
the resulting `ExtensionCallError`.

The implementation MUST classify token-endpoint HTTP failures as follows:

- `408`, `429`, `500`, `502`, `503`, and `504` MUST be retryable through the existing outer retry
  loop
- `400`, `401`, `403`, and `404` MUST be terminal through the existing outer retry loop

Transient token-endpoint failures MUST map as follows:

- token-endpoint request timeout: `408`
- token-endpoint connect failure: `503`
- token-endpoint I/O failure: `503`
- token-endpoint unexpected local exception: `500`

Malformed token responses MUST map to `502`.

Local signing, key-loading, and local auth-material failures MUST map to `500`.

## Configuration Model

The configuration model MUST stay close to the current
`community/participant/src/main/scala/com/digitalasset/canton/participant/config/ExtensionServiceConfig.scala`
shape.

The implementation MUST add an auth model. The implementation MUST NOT introduce a broad transport
configuration abstraction for participant extensions.

### Resource-Server Fields

The per-extension configuration MUST expose resource-server endpoint fields directly at the top
level.

The per-extension configuration MUST NOT nest the resource-server endpoint under an `endpoint`
block.

The top-level per-extension config MUST continue to expose the following resource-server fields
directly at the top level:

- `name`
- `host`
- `port`
- `use-tls`
- `trust-collection-file` when a custom trust collection is required
- `tls-insecure` as an existing test-only compatibility field for the resource-server endpoint
- `connect-timeout`
- `request-timeout`
- `max-total-timeout`
- `max-retries`
- `retry-initial-delay`
- `retry-max-delay`
- `request-id-header`
- `declared-functions`, which MUST remain supported and MUST default to empty when omitted

### Auth Fields

The auth configuration MUST be a typed variant. The implementation MUST NOT use `auth.mode` plus a
nested OAuth block.

The auth configuration MUST support:

- `auth.type = none`
- `auth.type = oauth`

`auth.type = none` MUST disable auth header injection.

`auth.type = oauth` MUST support:

- `token-endpoint`
  - `host`
  - `port`
  - `path`
  - `trust-collection-file` when a custom trust collection is required
  - `tls-insecure` only when test scaffolding requires insecure TLS for the token endpoint
- `client-id`
- `private-key-file`
- `key-id` when `kid` emission is required
- `scope` when the token endpoint requires scope

When `auth.type = oauth`, the resource server MUST use TLS.

When `auth.type = oauth`, the token endpoint MUST use TLS.

If a TLS endpoint provides `trust-collection-file`, the implementation MUST use that trust
collection for that endpoint.

If a TLS endpoint omits `trust-collection-file`, the implementation MUST use the JVM default trust
store for that endpoint.

The token endpoint path MUST start with `/`.

The token endpoint path MUST NOT contain a query string.

The token endpoint path MUST NOT contain a fragment.

The implementation MUST use the token-endpoint URI both as:

- the HTTP target for token acquisition
- the `aud` claim in the client assertion

The implementation MUST NOT support a separate assertion-audience override in v1.

The existing top-level `tls-insecure` field MUST apply only to the resource-server endpoint.

The token endpoint MUST NOT inherit the resource-server `tls-insecure` setting.

The implementation MUST treat insecure or trust-all TLS behavior as test-only scaffolding. The
canonical OAuth contract MUST NOT rely on insecure TLS.

### Global Extension Settings

The OAuth v1 specification MUST leave `EngineExtensionsConfig` behavior unchanged except where the
existing `echoMode` short-circuit already applies.

`echoMode` MUST continue to short-circuit HTTP calls.

The OAuth v1 specification MUST NOT define startup validation semantics for
`validateExtensionsOnStartup` or `failOnExtensionValidationError`.

## Example Configuration

The canonical configuration shape MUST be:

```hocon
extensions = {
  test-ext = {
    name = "test-ext"

    host = "ext.example.internal"
    port = 443
    use-tls = true
    trust-collection-file = "/etc/canton/ext-ca.pem"

    auth = {
      type = oauth

      token-endpoint = {
        host = "issuer.example.internal"
        port = 443
        path = "/oauth2/token"
        trust-collection-file = "/etc/canton/issuer-ca.pem"
      }

      client-id = "participant1"
      private-key-file = "/etc/canton/oauth-client-key.der"
      key-id = "participant1-key"
      scope = "external.call.invoke"
    }

    connect-timeout = 500ms
    request-timeout = 8s
    max-total-timeout = 25s
    max-retries = 3
    retry-initial-delay = 1s
    retry-max-delay = 10s
    request-id-header = "X-Request-Id"
    declared-functions = []
  }
}
```

## Error Mapping

The OAuth implementation MUST preserve the existing external-call error boundary.

Internal OAuth failures MUST map to `ExtensionCallError`.

`ExtensionServiceExternalCallHandler` MUST continue to expose only:

- `statusCode`
- `message`
- `requestId`

Resource-server transport and application failures MUST preserve the existing mapping already used
by `HttpExtensionServiceClient`.

The implementation MUST apply the following OAuth-specific mappings:

- resource-server `401` with `auth.type = none`: `401`, terminal, no replay, with auth-neutral
  message `Unauthorized`
- token-endpoint HTTP failure: preserve the exact HTTP status code
- malformed token response: `502`
- local signing failure: `500`
- local key-loading failure: `500`
- local auth-material failure: `500`
- resource-server token rejection after the auth-local replay is exhausted: `401` with message
  `Unauthorized - OAuth token rejected by resource server`

The returned `requestId` MUST be the request ID of the outbound HTTP interaction that produced the
returned error.

If token acquisition or any local OAuth-material initialization step fails before any outbound HTTP
interaction is sent, the returned `requestId` MUST be `None`.

## Logging and Metrics

The implementation MUST emit structured logs for:

- token acquisition start
- token acquisition success
- token acquisition failure
- cache reuse
- token reacquisition
- token invalidation after resource-server `401`
- final external-call failure classification

The implementation MUST NOT log:

- access tokens
- client assertions
- private key material
- token-endpoint request bodies

The canonical OAuth v1 implementation defines no OAuth-specific metrics.

The implementation MUST NOT depend on new metrics for correctness or acceptance.

## Test Requirements

The implementation MUST add or update unit tests for:

- typed auth config parsing
- token request construction
- client assertion construction
- token acquisition success
- token acquisition failure
- cache reuse
- expiry-driven reacquisition
- `401` invalidate-and-replay
- key-loading failures
- trust-material failures

If implementation work uncovers a bug, the implementation MUST add a regression unit test for that
failing scenario.

### Testing Strategy

The OAuth v1 test strategy MUST be comprehensive on the specified contract while remaining minimal,
upstream-friendly, and scoped to OAuth v1 behavior.

The test suite MUST cover:

- happy-path behavior
- edge cases that affect config parsing, request construction, retries, deadlines, caching,
  invalidation, replay, concurrency, and error mapping
- relevant security behavior required by this specification

Fine-grained protocol, parsing, retry, deadline, cache, concurrency, and error-mapping scenarios
MUST be covered primarily by unit tests.

Integration tests MUST focus on representative end-to-end behavior and MUST NOT duplicate every
failure permutation already covered by unit tests.

Tests MUST assert observable contract behavior and MUST NOT depend on incidental implementation
details such as the exact compact JWT serialization beyond the required claims and headers.

Implementation work MUST proceed in explicit TDD red-green cycles for each test slice:

- first add or update the most specific targeted test coverage for the next behavior slice
- then run the most specific relevant suite and observe the expected failing result
- then implement the minimum production change needed for that slice
- then rerun the same relevant suite to confirm the slice is green
- then update the living checklist to reflect the new coverage state

The repository MAY be red during an active local red-green cycle, but it MUST be returned to a
green state before pausing, handing off, or reporting a completed slice.

The suite MUST include security-relevant coverage required by this specification, including:

- TLS-related constraints that OAuth requires
- required token-request fields and forbidden omissions or additions
- required client-assertion claims and header behavior
- malformed token-response handling
- failures to load or initialize key and trust material
- non-disclosure of access tokens, client assertions, and private key material in logs

The suite MUST NOT add coverage for capabilities explicitly excluded from OAuth v1.

The living implementation checklist for this test strategy is
`community/participant/EXTERNAL_CALL_OAUTH_TEST_PLAN.md`, and the coding agent MUST keep that
document updated during implementation.

The implementation MUST reuse the existing external-call integration harness:

- `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/ExternalCallIntegrationTestBase.scala`
- `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/MockExternalCallServer.scala`

`MockExternalCallServer` MUST be extended to serve both:

- `/api/v1/external-call`
- the configured token-endpoint path used by OAuth tests

OAuth integration tests MUST terminate HTTPS for both the resource endpoint and the token endpoint.

OAuth integration tests MAY satisfy TLS verification either by:

- providing explicit test trust material through `trust-collection-file`
- using the test-only `tls-insecure` and `token-endpoint.tls-insecure` scaffolding

The canonical example configuration remains production-oriented and therefore MUST NOT rely on the
test-only insecure TLS flags.

The integration suite MUST cover:

- `auth.type = none`
- end-to-end OAuth success
- cached-token reuse across multiple business requests
- expiry-driven reacquisition on the next request
- single `401` refresh-and-replay
- submission and validation producing the same successful business response under OAuth

The integration suite MUST keep the existing non-OAuth `401` behavior covered separately. The
OAuth-specific replay MUST NOT redefine non-OAuth `401` handling.

## Exclusions

The OAuth v1 implementation MUST NOT define or require:

- startup validation integration
- participant startup gating
- local-only startup validation as a required behavior
- token-request `audience`
- proactive refresh
- background refresh tasks
- sender-constrained tokens
- mTLS-bound access tokens
- generic auth-provider interfaces
- a broad transport-config refactor for participant extensions
- hot reload of key material
- hot reload of trust material
