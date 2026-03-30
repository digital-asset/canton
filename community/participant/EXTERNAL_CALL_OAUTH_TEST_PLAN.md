# External Call OAuth Test Plan

## Purpose

This document is the living implementation checklist for the OAuth v1 test suite described in
`community/participant/EXTERNAL_CALL_OAUTH_TECH_SPEC.md`.

It is intended to guide implementation, track coverage, and prevent test scope from drifting away
from the agreed OAuth v1 contract.

## Maintenance Rules For The Coding Agent

- Update this file in the same change whenever OAuth-related tests are added, removed, renamed,
  split, deferred, or expanded.
- Execute each implemented behavior slice as an explicit TDD cycle:
  write or update the test first, run the most specific relevant suite to observe the expected
  failure, implement the minimum production change, rerun the same suite to green, then update this
  file.
- Add a new checkbox item for every regression or bug discovered during implementation before, or at
  the same time as, the fix.
- Mark an item `[x]` only when an automated test exists in the repository and the most specific
  relevant suite has been run successfully.
- If a test exists but is blocked, flaky, or not yet run in the relevant suite, leave the item
  unchecked and record the blocker in `Implementation Notes`.
- Do not leave intentionally failing tests in the repository as a resting state. The repository may
  go red during an active local TDD cycle, but it must be green again before pausing or handing
  work off.
- Keep each checkbox scoped to one observable behavior. If one parameterized test covers multiple
  checklist items, keep all covered items and note the shared test name in `Implementation Notes`.
- Do not silently delete or collapse checklist items. If scope changes, replace the old item with
  the new one and explain the change in `Implementation Notes`.
- Keep candidate test-file paths in this document current as the implementation evolves.
- Keep this document focused on OAuth v1. Do not add checklist items for behavior explicitly
  excluded by the tech spec.
- Record the concrete failing and passing test command for each completed implementation slice in
  `Implementation Notes`, unless the existing note for that slice is still accurate.

## Status Legend

- `[ ]` Planned or still missing
- `[x]` Implemented and verified in the relevant automated test suite

## Candidate Test Files

- `community/participant/src/test/scala/com/digitalasset/canton/participant/config/ExtensionServiceConfigOAuthTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/HttpExtensionRequestBuilderOAuthTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/HttpExtensionOAuthTokenRequestBuilderTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/HttpExtensionOAuthClientAssertionFactoryTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/HttpExtensionOAuthTokenResponseParserTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/HttpExtensionOAuthTokenClientTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/HttpExtensionServiceClientOAuthTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/JdkHttpExtensionClientResourcesFactoryOAuthTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/ExtensionServiceExternalCallHandlerOAuthTest.scala`
- `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/OAuthExternalCallIntegrationTest.scala`

The exact file split may change during implementation, but the checklist coverage below must remain
intact.

## Out Of Scope For OAuth v1

Do not add checklist items for:

- startup validation integration or participant startup gating as a required OAuth behavior
- token-request `audience`
- sender-constrained or mTLS-bound access tokens
- proactive or background token refresh
- hot reload of key material
- hot reload of trust material
- a broad auth-provider abstraction
- a broad transport abstraction for participant extensions

## Checklist

### 1. Config Model And Parsing

- [x] Parse `auth.type = none` with existing top-level resource-server fields unchanged.
- [x] Parse `auth.type = oauth` with all required OAuth fields present.
- [x] Reject missing `auth.type`.
- [x] Reject unknown `auth.type`.
- [x] Reject `auth.type = oauth` when `client-id` is missing.
- [x] Reject `auth.type = oauth` when `private-key-file` is missing.
- [x] Reject `auth.type = oauth` when `token-endpoint.host` is missing.
- [x] Reject `auth.type = oauth` when `token-endpoint.port` is missing.
- [x] Reject `auth.type = oauth` when `token-endpoint.path` is missing.
- [x] Reject token-endpoint paths that do not start with `/`.
- [x] Reject token-endpoint paths that contain a query string.
- [x] Reject token-endpoint paths that contain a fragment.
- [x] Reject `auth.type = oauth` when the resource server is not configured for TLS.
- [x] Construct the OAuth token-endpoint URI as `https://<host>:<port><path>` with no configurable non-TLS scheme.
- [x] Parse optional `key-id` when present.
- [x] Parse optional `scope` when present.
- [x] Omit optional `scope` when absent.
- [x] Parse token-endpoint `tls-insecure` when present for test scaffolding.
- [x] Parse resource-server `trust-collection-file` independently from token-endpoint `trust-collection-file`.
- [x] Preserve `declared-functions` support and its empty-default behavior.
- [x] Preserve existing non-auth top-level fields without introducing an `endpoint` wrapper.

### 2. Resource Request Construction

- [x] Preserve `POST /api/v1/external-call`.
- [x] Preserve `Content-Type: application/octet-stream`.
- [x] Preserve `X-Daml-External-Function-Id`.
- [x] Preserve `X-Daml-External-Config-Hash`.
- [x] Preserve `X-Daml-External-Mode`.
- [x] Preserve the configured request ID header on resource requests.
- [x] Preserve the business request body format unchanged.
- [x] Preserve the successful business response body format unchanged.
- [x] With `auth.type = none`, omit the `Authorization` header.
- [x] With `auth.type = oauth`, add `Authorization: Bearer <token>`.
- [x] Forward `mode = submission` unchanged.
- [x] Forward `mode = validation` unchanged.
- [ ] Clamp the resource request timeout to `min(configured request-timeout, remaining budget)`.

### 3. Token Request Construction

- [x] Send token acquisition to the configured token-endpoint URI.
- [ ] Use HTTP `POST` for token acquisition.
- [x] Use `Content-Type: application/x-www-form-urlencoded`.
- [x] Attach a participant-generated request ID using the configured request ID header.
- [x] Include `grant_type = client_credentials`.
- [x] Include `client_assertion_type = urn:ietf:params:oauth:client-assertion-type:jwt-bearer`.
- [x] Include `client_assertion = <signed JWT>`.
- [x] Include `scope` when configured.
- [x] Omit `scope` when not configured.
- [x] Omit token-request `audience`.
- [x] Use the token-endpoint URI both as the HTTP target and as the client-assertion `aud`.

### 4. Client Assertion Construction

- [x] Use signing algorithm `RS256`.
- [x] Set `iss = client-id`.
- [x] Set `sub = client-id`.
- [x] Set `aud = <token-endpoint URI>`.
- [x] Set `iat = now`.
- [x] Set `exp = now + 30s`.
- [x] Set `jti` to a fresh random identifier.
- [x] Include `kid` when `key-id` is configured.
- [x] Omit `kid` when `key-id` is not configured.
- [x] Produce one-use-only assertions across successive acquisitions.
- [x] Accept RSA DER / PKCS#8 key material for signing.

### 5. Token Response Parsing And Acquisition

- [x] Accept a valid token response containing `access_token`, `token_type`, and `expires_in`.
- [x] Accept `token_type = Bearer` case-insensitively.
- [x] Reject token responses missing `access_token`.
- [x] Reject token responses missing `token_type`.
- [x] Reject token responses missing `expires_in`.
- [x] Reject malformed `expires_in`.
- [x] Reject non-Bearer `token_type`.
- [x] Treat malformed token responses as `502`.
- [x] Compute local token expiry from `expires_in`.
- [x] Treat access tokens as opaque bearer tokens without local claim parsing or verification.

### 6. Token Cache Behavior

- [ ] Reuse an unexpired cached token on later business requests for the same extension.
- [ ] Reacquire a token on the next business request after local expiry.
- [ ] Perform no proactive refresh before a business request needs a token.
- [ ] Perform no background refresh work.
- [ ] Invalidate a cached token after a resource-server `401` only when that same token was sent.
- [ ] Reacquire a fresh token after invalidation and replay the resource request exactly once.
- [ ] Return success when the replay succeeds.
- [ ] Return terminal `401` with the OAuth-specific rejection message when the replay also gets `401`.
- [ ] Do not perform a second auth-local replay after the first replay is exhausted.
- [ ] Trigger auth-local invalidate-and-replay only on resource `401`.
- [ ] Preserve a newer cached token if a late `401` arrives for an older token value.
- [ ] Store and reuse the freshly acquired token after a successful replay.

### 7. Concurrency And Serialization

- [ ] Serialize concurrent cold-cache misses so only one token acquisition is in flight per extension.
- [ ] Make concurrent requests wait for an in-flight token acquisition instead of starting a second one.
- [ ] Serialize concurrent refreshes caused by expiry.
- [ ] Serialize concurrent refreshes caused by resource-server `401`.
- [ ] Propagate a shared refresh success to all waiting requests.
- [ ] Propagate a shared refresh failure to all waiting requests.
- [ ] Allow a new acquisition attempt after a prior shared refresh failure has completed.
- [ ] Keep token caches isolated per extension.

### 8. Retry And Deadline Model

- [ ] Compute one absolute deadline per external-call operation from `max-total-timeout`.
- [ ] Refuse to start token acquisition when the remaining budget is non-positive.
- [ ] Refuse to start a resource request when the remaining budget is non-positive.
- [ ] Count only outer retries against `maxRetries`.
- [ ] Do not charge the auth-local replay against `maxRetries`.
- [x] Continue to classify resource `400`, `401`, `403`, and `404` as terminal.
- [ ] Continue to classify resource `408`, `429`, `500`, `502`, `503`, and `504` as retryable.
- [ ] Retry token-endpoint `408`, `429`, `500`, `502`, `503`, and `504` through the same outer retry loop.
- [ ] Treat token-endpoint `400`, `401`, `403`, and `404` as terminal through the same outer retry loop.
- [ ] Preserve `Retry-After` handling for retryable token-endpoint failures.
- [ ] Preserve exponential backoff behavior for retryable failures without `Retry-After`.
- [ ] Feed the replay result back into the normal outer retry classification after the one allowed replay.
- [ ] Stop retrying when insufficient remaining time exists for another attempt.
- [ ] Clamp token-request timeout to the remaining budget.
- [ ] Keep `connect-timeout` fixed per client and do not dynamically clamp it per attempt.
- [ ] Consume total budget across token acquisition, resource request, replay, and outer retries.

### 9. Error Mapping And Error Boundary

- [x] With `auth.type = none`, map resource `401` to terminal `401` with auth-neutral message `Unauthorized`.
- [x] With `auth.type = none`, do not trigger OAuth replay behavior on resource `401`.
- [x] Preserve exact HTTP status codes for token-endpoint HTTP failures.
- [x] Map token-endpoint request timeout to `408`.
- [x] Map token-endpoint connect failure to `503`.
- [x] Map token-endpoint I/O failure to `503`.
- [x] Map token-endpoint unexpected local exception to `500`.
- [x] Map local signing failure to `500`.
- [x] Map local key-loading failure to `500`.
- [ ] Map local auth-material failure to `500`.
- [ ] After replay exhaustion, map resource token rejection to `401` with message `Unauthorized - OAuth token rejected by resource server`.
- [x] Return the outbound request ID of the interaction that produced the final error.
- [x] Return `requestId = None` when failure occurs before any outbound HTTP interaction is sent.
- [ ] Preserve the existing `ExtensionServiceExternalCallHandler` boundary of `statusCode`, `message`, and `requestId` only.

### 10. HTTP Client Ownership, TLS, And Material Initialization

- [ ] With `auth.type = none`, create exactly one internal HTTP client/transport for resource requests.
- [ ] With `auth.type = oauth`, create separate internal HTTP clients/transports for resource and token requests.
- [ ] Keep HTTP client ownership per extension with no required cross-extension sharing.
- [ ] Do not load signing key, trust material, or OAuth-specific HTTP client state during `HttpExtensionServiceClient` construction.
- [ ] Do not fail client construction solely because OAuth key or trust material is invalid before the first OAuth use.
- [ ] Load signing key, trust material, and OAuth-specific HTTP client state on demand rather than at construction time.
- [ ] Reuse successfully initialized signing key material for the lifetime of the `HttpExtensionServiceClient`.
- [ ] Reuse successfully initialized trust material for the lifetime of the `HttpExtensionServiceClient`.
- [ ] Reuse successfully initialized OAuth-specific HTTP client state for the lifetime of the `HttpExtensionServiceClient`.
- [ ] Do not re-read key material on every token request.
- [ ] Do not re-read trust material on every token request.
- [ ] Keep resource-endpoint and token-endpoint trust configuration independent.
- [ ] Ensure top-level resource `tls-insecure` does not apply to the token endpoint.
- [ ] Use endpoint-specific custom trust material when configured.
- [ ] Fall back to the JVM default trust store when endpoint-specific trust material is omitted.
- [ ] Treat insecure or trust-all TLS as test-only scaffolding rather than the canonical contract.

### 11. Logging And Secret Redaction

- [ ] Log token acquisition start.
- [ ] Log token acquisition success.
- [ ] Log token acquisition failure.
- [ ] Log cache reuse.
- [ ] Log token reacquisition.
- [ ] Log token invalidation after resource-server `401`.
- [ ] Log final external-call failure classification.
- [ ] Do not log access tokens.
- [ ] Do not log client assertions.
- [ ] Do not log private key material.
- [ ] Do not log token-endpoint request bodies.

### 12. Integration Coverage

- [ ] Keep existing `auth.type = none` behavior covered separately from OAuth-specific behavior.
- [ ] Keep existing non-OAuth `401` no-replay behavior covered separately from OAuth-specific replay behavior.
- [ ] Cover end-to-end OAuth success over HTTPS for both the resource endpoint and the token endpoint.
- [ ] Cover cached-token reuse across multiple business requests.
- [ ] Cover expiry-driven reacquisition on the next business request.
- [ ] Cover single `401` refresh-and-replay.
- [ ] Cover submission and validation producing the same successful business response under OAuth.
- [ ] Cover token-endpoint retryable failure followed by success through the outer retry loop.
- [ ] Cover token-endpoint terminal failure surfacing the preserved HTTP status.
- [ ] Cover malformed token response surfacing `502`.
- [ ] Cover local key-loading failure before outbound HTTP with `requestId = None`.
- [ ] Cover local trust-material failure before outbound HTTP with `requestId = None`.
- [ ] Preserve participant startup with OAuth configured even when OAuth key or trust material is invalid until the first OAuth call path is exercised.
- [ ] Do not send token-endpoint HTTP interactions during participant startup or extension-manager construction.
- [ ] Preserve existing startup validation semantics without introducing OAuth-specific startup gating.
- [ ] Cover test-only insecure TLS scaffolding when the integration harness relies on it.
- [ ] Cover explicit trust-material configuration when the integration harness relies on custom test trust roots.
- [ ] Preserve `echoMode` short-circuit behavior without token or resource HTTP calls.
- [ ] Extend the mock server and harness to serve both `/api/v1/external-call` and the configured token-endpoint path.

### 13. Regression Additions

- [ ] Add a new checkbox item here for each OAuth-specific bug discovered during implementation.

## Implementation Notes

- Add dated notes here for blockers, scope changes, shared parameterized tests, or renamed test files.
- Do not remove historical notes without replacing them with an updated note.
- 2026-03-30: `ExtensionServiceConfigOAuthTest` covers the implemented config parsing and validation items in sections 1 and the parameterized invalid-field and invalid-path cases.
- 2026-03-30: `HttpExtensionRequestBuilderOAuthTest` covers the implemented resource-request construction items in section 2, including auth-none omission and explicit bearer header injection.
- 2026-03-30: `HttpExtensionOAuthTokenRequestBuilderTest` covers the implemented token-request construction items in section 3 except the explicit HTTP `POST` observable, which remains open for a later slice. The same targeted command was used for the explicit red-green cycle: `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionOAuthTokenRequestBuilderTest'` first failed with `not found: type HttpExtensionOAuthTokenRequestBuilder`, then passed after adding the builder.
- 2026-03-30: `HttpExtensionOAuthClientAssertionFactoryTest` covers the implemented client-assertion construction items in section 4 and, together with `HttpExtensionOAuthTokenRequestBuilderTest`, closes the shared section 3 item requiring the token-endpoint URI to be used both as the HTTP target and as the client-assertion `aud`. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionOAuthClientAssertionFactoryTest'`, which first failed with `not found: type HttpExtensionOAuthClientAssertionFactory`, then passed after adding the factory.
- 2026-03-30: `HttpExtensionOAuthTokenResponseParserTest` covers the implemented token-response parsing items in section 5, including malformed-response `502` mapping and opaque-token handling. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionOAuthTokenResponseParserTest'`, which first failed with `not found: type HttpExtensionOAuthTokenResponseParser`, then exposed one intermediate red case where the parser was still too permissive for `expires_in`, and finally passed after tightening field-shape validation.
- 2026-03-30: `HttpExtensionOAuthTokenClientTest` covers the implemented acquisition helper behavior and section 9 token-endpoint error mapping items for preserved HTTP status codes, transport exception mapping, local signing/key-loading failures, and `requestId` presence or absence depending on whether any outbound HTTP interaction occurred. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionOAuthTokenClientTest'`, which first failed with `not found: type HttpExtensionOAuthTokenClient`, then passed after adding the client.
- 2026-03-30: `HttpExtensionServiceClientTest` covers the currently verified auth-none `401` behavior and terminal classification for resource `400`, `401`, `403`, and `404`.
- 2026-03-30: Verified with `sbt 'community-participant/testOnly com.digitalasset.canton.participant.config.ExtensionServiceConfigOAuthTest com.digitalasset.canton.participant.extension.HttpExtensionRequestBuilderOAuthTest com.digitalasset.canton.participant.extension.HttpExtensionServiceClientTest com.digitalasset.canton.participant.extension.ExtensionServiceManagerTest com.digitalasset.canton.participant.extension.JdkHttpExtensionClientResourcesFactoryTest com.digitalasset.canton.participant.extension.ExtensionServiceExternalCallHandlerTest'`.
