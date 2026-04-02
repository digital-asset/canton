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
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/JdkHttpExtensionClientResourcesFactoryTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/ExtensionServiceExternalCallHandlerTest.scala`
- `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/OAuthExternalCallIntegrationTest.scala`
- `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/BasicExternalCallIntegrationTest.scala`
- `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/OAuthExternalCallStartupIntegrationTest.scala`
- `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/OAuthExternalCallStartupValidationIntegrationTest.scala`
- `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/OAuthEchoModeExternalCallIntegrationTest.scala`

The exact file split may change during implementation, but the checklist coverage below must remain
intact.

## Out Of Scope For OAuth v1

Do not add checklist items for:

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
- [x] Clamp the resource request timeout to `min(configured request-timeout, remaining budget)`.

### 3. Token Request Construction

- [x] Send token acquisition to the configured token-endpoint URI.
- [x] Use HTTP `POST` for token acquisition.
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
- [x] Accept a valid token response containing `access_token` and `token_type` when `expires_in` is omitted.
- [x] Accept `token_type = Bearer` case-insensitively.
- [x] Reject token responses missing `access_token`.
- [x] Reject token responses missing `token_type`.
- [x] Reject malformed `expires_in`.
- [x] Reject non-Bearer `token_type`.
- [x] Treat malformed token responses as `502`.
- [x] Compute local token expiry from `expires_in` when it is present.
- [x] Treat access tokens as opaque bearer tokens without local claim parsing or verification.

### 6. Token Cache Behavior

- [x] Reuse an unexpired cached token on later business requests for the same extension.
- [x] Do not place tokens without `expires_in` into the shared cache for later business requests.
- [x] Reacquire a token on the next business request after local expiry.
- [x] Perform no proactive refresh before a business request needs a token.
- [x] Perform no background refresh work.
- [x] Invalidate a cached token after a resource-server `401` only when that same token was sent.
- [x] Reacquire a fresh token after invalidation and replay the resource request exactly once.
- [x] Return success when the replay succeeds.
- [x] Return terminal `401` with the OAuth-specific rejection message when the replay also gets `401`.
- [x] Do not perform a second auth-local replay after the first replay is exhausted.
- [x] Trigger auth-local invalidate-and-replay only on resource `401`.
- [x] Preserve a newer cached token if a late `401` arrives for an older token value.
- [x] Store and reuse the freshly acquired token after a successful replay.

### 7. Concurrency And Serialization

- [x] Serialize concurrent cold-cache misses so only one token acquisition is in flight per extension.
- [x] Make concurrent requests wait for an in-flight token acquisition instead of starting a second one.
- [x] Serialize concurrent refreshes caused by expiry.
- [x] Serialize concurrent refreshes caused by resource-server `401`.
- [x] Propagate a shared refresh success to all waiting requests.
- [x] Propagate a shared refresh failure to all waiting requests.
- [x] Allow a new acquisition attempt after a prior shared refresh failure has completed.
- [x] Keep token caches isolated per extension.

### 8. Retry And Deadline Model

- [x] Compute one absolute deadline per external-call operation from `max-total-timeout`.
- [x] Refuse to start token acquisition when the remaining budget is non-positive.
- [x] Refuse to start a resource request when the remaining budget is non-positive.
- [x] Count only outer retries against `maxRetries`.
- [x] Do not charge the auth-local replay against `maxRetries`.
- [x] Continue to classify resource `400`, `401`, `403`, and `404` as terminal.
- [x] Continue to classify resource `408`, `429`, `500`, `502`, `503`, and `504` as retryable.
- [x] Retry token-endpoint `408`, `429`, `500`, `502`, `503`, and `504` through the same outer retry loop.
- [x] Treat token-endpoint `400`, `401`, `403`, and `404` as terminal through the same outer retry loop.
- [x] Preserve `Retry-After` handling for retryable token-endpoint failures.
- [x] Preserve exponential backoff behavior for retryable failures without `Retry-After`.
- [x] Feed the replay result back into the normal outer retry classification after the one allowed replay.
- [x] Stop retrying when insufficient remaining time exists for another attempt.
- [x] Clamp token-request timeout to the remaining budget.
- [x] Clamp resource-request timeout to the remaining budget.
- [x] Keep `connect-timeout` fixed per client and do not dynamically clamp it per attempt.
- [x] Consume total budget across token acquisition, resource request, replay, and outer retries.

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
- [x] Map local auth-material failure to `500`.
- [x] After replay exhaustion, map resource token rejection to `401` with message `Unauthorized - OAuth token rejected by resource server`.
- [x] Return the outbound request ID of the interaction that produced the final error.
- [x] Return `requestId = None` when failure occurs before any outbound HTTP interaction is sent.
- [x] Preserve the existing `ExtensionServiceExternalCallHandler` boundary of `statusCode`, `message`, and `requestId` only.

### 10. HTTP Client Ownership, TLS, And Material Initialization

- [x] With `auth.type = none`, create exactly one internal HTTP client/transport for resource requests.
- [x] With `auth.type = oauth`, create separate internal HTTP clients/transports for resource and token requests.
- [x] Keep HTTP client ownership per extension with no required cross-extension sharing.
- [x] Do not load signing key during `HttpExtensionServiceClient` construction.
- [x] Do not load trust material during `HttpExtensionServiceClient` construction.
- [x] Do not load OAuth-specific HTTP client state during `HttpExtensionServiceClient` construction.
- [x] Do not fail client construction solely because the OAuth signing key is invalid before the first OAuth use.
- [x] Do not fail client construction solely because OAuth trust material is invalid before the first OAuth use.
- [x] Do not fail client construction solely because OAuth-specific HTTP client state is invalid before the first OAuth use.
- [x] Load signing key on demand rather than at construction time.
- [x] Load trust material on demand rather than at construction time.
- [x] Load OAuth-specific HTTP client state on demand rather than at construction time.
- [x] Reuse successfully initialized signing key material for the lifetime of the `HttpExtensionServiceClient`.
- [x] Reuse successfully initialized trust material for the lifetime of the `HttpExtensionServiceClient`.
- [x] Reuse successfully initialized OAuth-specific HTTP client state for the lifetime of the `HttpExtensionServiceClient`.
- [x] Do not re-read key material on every token request.
- [x] Do not re-read trust material on every token request.
- [x] Keep resource-endpoint and token-endpoint `tls-insecure` configuration independent.
- [x] Keep resource-endpoint and token-endpoint custom trust-material configuration independent.
- [x] Ensure top-level resource `tls-insecure` does not apply to the token endpoint.
- [x] Use endpoint-specific custom trust material when configured.
- [x] Fall back to the JVM default trust store when endpoint-specific trust material is omitted.
- [x] Run startup local preflight without sending outbound HTTP.
- [x] Fail startup local preflight on invalid OAuth signing key material.
- [x] Fail startup local preflight on invalid OAuth trust material.
- [x] Fail startup local preflight when OAuth client construction cannot produce the dedicated token transport.
- [x] Build one OAuth client assertion during startup local preflight.

### 11. Logging And Secret Redaction

- [x] Log token acquisition start.
- [x] Log token acquisition success.
- [x] Log token acquisition failure.
- [x] Log cache reuse.
- [x] Log token reacquisition.
- [x] Log token invalidation after resource-server `401`.
- [x] Log final external-call failure classification.
- [x] Do not log access tokens.
- [x] Do not log client assertions.
- [x] Do not log private key material.
- [x] Do not log token-endpoint request bodies.

### 12. Integration Coverage

- [x] Keep existing `auth.type = none` behavior covered separately from OAuth-specific behavior.
- [x] Keep existing non-OAuth `401` no-replay behavior covered separately from OAuth-specific replay behavior.
- [x] Cover end-to-end OAuth success over HTTPS for both the resource endpoint and the token endpoint.
- [x] Cover cached-token reuse across multiple business requests.
- [x] Cover expiry-driven reacquisition on the next business request.
- [x] Cover single `401` refresh-and-replay.
- [x] Cover submission and validation producing the same successful business response under OAuth.
- [x] Cover token-endpoint retryable failure followed by success through the outer retry loop.
- [x] Cover token-endpoint terminal failure surfacing the preserved HTTP status.
- [x] Cover malformed token response surfacing `502`.
- [x] Cover local key-loading failure before outbound HTTP with `requestId = None`.
- [x] Cover local trust-material failure before outbound HTTP with `requestId = None`.
- [x] Fail participant startup on invalid local OAuth key material before any startup HTTP.
- [x] Fail participant startup on invalid local OAuth trust material before any startup HTTP.
- [x] Skip startup remote validation when `validateExtensionsOnStartup = false`.
- [x] Perform OAuth startup remote validation by acquiring a token and then sending an authenticated resource validation request when `validateExtensionsOnStartup = true`.
- [x] Continue startup after invalid startup remote validation when `validateExtensionsOnStartup = true` and `failOnExtensionValidationError = false`.
- [x] Fail startup after invalid startup remote validation when `validateExtensionsOnStartup = true` and `failOnExtensionValidationError = true`.
- [x] Keep the integration harness on explicit trust roots instead of relying on test-only insecure TLS scaffolding.
- [x] Cover explicit trust-material configuration when the integration harness relies on custom test trust roots.
- [x] Preserve `echoMode` short-circuit behavior without token or resource HTTP calls.
- [x] Extend the mock server and harness to serve both `/api/v1/external-call` and the configured token-endpoint path.

### 13. Regression Additions

- [x] Keep concurrent OAuth test transports thread-safe so shared refresh and acquisition tests measure client behavior rather than fixture races.
- [x] Bootstrap the shared external-call integration synchronizer with `topologyChangeDelay = 0` under simulated or remote clocks so participants become active before the test connects them.
- [x] Wire the ledger-api submission path through `ExternalCallHandler` so submission can materialize external-call results before replay.
- [x] Keep OAuth integration expectations aligned with submission-plus-validation execution, which produces two resource calls per business request on the confirming participant.
- [x] Drive OAuth integration expiry and deadline decisions from the participant clock rather than `System.currentTimeMillis()` so sim-clock advancement exercises the real OAuth cache path.
- [x] Isolate shared-environment OAuth integration slices by extension ID so warm token caches from earlier examples do not leak into later per-slice assertions.
- [x] Allow outer retries to proceed whenever positive budget remains, with the next request timeout clamped to that smaller remaining budget instead of requiring room for `connect-timeout + configured request-timeout`.
- [x] Fail locally in OAuth mode when the resources bundle omits the dedicated token transport, instead of silently falling back to the resource transport.
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
- 2026-03-30: `HttpExtensionServiceClientOAuthTest` covers the implemented section 6 token-cache behavior for first-call acquisition, unexpired cache reuse, expiry-driven reacquisition, and the absence of proactive or background refresh work. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, which first failed because `HttpExtensionClientResources` and `HttpExtensionServiceClient` did not yet expose the OAuth seams needed for the slice, then passed after adding optional token transport support and an OAuth token cache to `HttpExtensionServiceClient`.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the implemented section 6 and section 9 replay behavior for single `401` invalidate-refresh-replay, replay exhaustion with the OAuth-specific terminal `401` message, and reuse of the freshly acquired token after a successful replay. The explicit red-green command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`: the new examples first failed against the pre-replay service-client path, then passed after adding the auth-local replay flow.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the implemented section 8 deadline behavior for clamping token and resource request timeouts to the remaining outer budget and refusing to start the resource request once token acquisition has exhausted that budget. The explicit red-green command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`: after fixing one missing `Duration` import in the test, the new examples failed against the pre-deadline service-client path and then passed after threading the absolute deadline through auth resolution and resource sends.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the implemented section 9 and section 10 behavior for lazy OAuth-specific HTTP client initialization and local auth-material failure mapping. The explicit red-green command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`: the first new example proved client construction stayed lazy, the second initially failed because the lazy initialization exception escaped the future, and it passed after mapping that pre-outbound failure to `500` with `requestId = None`. The error-boundary example fixes `maxRetries = 0` so it isolates the mapping instead of the retry policy.
- 2026-04-01: `JdkHttpExtensionClientResourcesFactoryTest` now covers the implemented section 10 transport-ownership split for `auth.type = none` versus `auth.type = oauth`. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.JdkHttpExtensionClientResourcesFactoryTest'`: the new OAuth example first failed because `tokenTransport` was still `None`, then passed after `JdkHttpExtensionClientResourcesFactory` began creating a distinct token transport for OAuth configs.
- 2026-04-01: `JdkHttpExtensionClientResourcesFactoryTest` now also covers the remaining section 3 HTTP-method item by sending a real token-transport request to the local HTTPS server and asserting that the server observed `POST`. The explicit TDD command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.JdkHttpExtensionClientResourcesFactoryTest'`, and this slice went green immediately without production changes because `JdkHttpExtensionClientTransport` already sends requests with `POST`.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also contains representative token-endpoint outer-loop examples for a retryable `503` with `Retry-After` and a terminal `400` that must not send the resource request. The explicit command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, and this slice went green immediately without production changes; the current implementation already satisfied these representative cases. The broader status-family checklist items in section 8 remain open until parameterized coverage is expanded beyond the `503` and `400` examples.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also contains representative section 6 and section 8 examples for non-`401` no-replay behavior, auth-local replay succeeding with `maxRetries = 0`, replaying into a retryable `503` that flows back through the outer retry loop, and token-endpoint `503` retry using exponential backoff when `Retry-After` is absent. The explicit command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, and this slice also went green immediately without production changes; the current implementation already satisfied these representative cases.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the implemented cold-cache concurrency behavior in section 7. The explicit red-green command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`: the first red exposed one order-sensitive success assertion plus the real bug where a waiting caller started a second token acquisition after the first failed, then the suite passed after switching `HttpExtensionServiceClient` to a shared in-flight token-acquisition promise. This slice closes only the cold-cache concurrency items; the refresh-specific concurrency items remain open until the same mechanism is exercised through expiry-driven and `401`-driven refresh paths.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the refresh-specific concurrency behavior in section 7 for expiry-driven shared refresh success, `401`-driven shared refresh success, `401`-driven shared refresh failure, and a later retry after that shared failure. The explicit command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, and this slice went green immediately without production changes because the shared in-flight token-acquisition promise added in the previous slice already generalized to refresh paths.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the remaining section 6 `401` invalidation guards: only invalidating when the rejected token is the one that was sent, and preserving a newer cached token when a late `401` arrives for an older token value. The explicit TDD command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest -- -z "preserve a newer cached token"'`: the first red was a test-compile failure because the helper factory still required `FakeTransport`, then a second red exposed thread-unsafe request-ID scaffolding in the concurrent test helper. The slice went green after generalizing the test helper transport type, making `FakeRuntime` request-ID access thread-safe, and tightening the new late-`401` example back to a strict six-request fixture. No production change was required for the OAuth client itself. The broader suite was then reverified with `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers per-extension cache isolation in section 7 by driving two OAuth clients against shared scripted transports and proving that each extension acquires and reuses its own token. The explicit command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest -- -z "keep token caches isolated per extension"'`, and this slice went green immediately without production changes. During the required full-suite rerun, an existing concurrent-`401` refresh example became red because `FakeTransport` was not thread-safe; fixing that test-harness race required synchronizing `FakeTransport.send`. The full suite then passed with `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`.
- 2026-04-01: `ExtensionServiceExternalCallHandlerTest` now also covers the section 9 boundary contract that the external-call handler preserves only `statusCode`, `message`, and `requestId` when mapping extension errors into the engine-facing `ExternalCallError`, even when the source error is `ExtensionCallErrorWithRetry`. The explicit TDD command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.ExtensionServiceExternalCallHandlerTest'`: the first red was a missing `FutureUnlessShutdown` import in the new test helper override, and the suite then went green immediately after adding that import with no production changes.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the remaining token-endpoint status-family items in section 8: terminal `401`, `403`, and `404` responses, and retryable `408`, `429`, `500`, `502`, `503`, and `504` responses flowing through the same outer retry loop. The explicit TDD command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`: the first red was test-side, where the expected token-endpoint `404` and `403` messages were still using resource-endpoint wording, and the required rerun also exposed that synchronizing the entire fake transport `send` path was too coarse because some concurrency hooks intentionally block inside `onSend`. The slice went green after correcting those test expectations and narrowing `FakeTransport` synchronization to just the mutable queue operations. No production change was required for the OAuth client itself.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the remaining resource retryable-status item in section 8 by parameterizing OAuth calls over resource `408`, `429`, `500`, `502`, `503`, and `504`, asserting one outer retry and cached-token reuse on the second attempt. The same rerun also reconfirmed the existing timeout-clamping examples, which closes the section 2 resource-request timeout clamp item. The explicit command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, and this slice went green immediately without production changes.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the remaining section 8 item requiring the client to refuse token acquisition when the outer deadline is already exhausted. The explicit command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, and this slice also went green immediately without production changes: with `maxTotalTimeout = 0`, the client returns `504` and sends neither token nor resource HTTP.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the remaining retry-budget items in section 8: auth-local `401` replays do not consume the outer `maxRetries` budget, and the client must stop retrying once the remaining budget cannot support another full outer attempt. The explicit TDD command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`: the new `maxRetries` example went green immediately, while the insufficient-budget example first failed because the client still retried with `0ms` backoff. The fix was in [HttpExtensionServiceClient.scala](/Users/al/Projects/angelo/zenith/full-stack/canton/community/participant/src/main/scala/com/digitalasset/canton/participant/extension/HttpExtensionServiceClient.scala), where the outer loop now checks for enough time for another attempt before scheduling a retry. Reverified with `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientTest com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` and `JdkHttpExtensionClientResourcesFactoryTest` now cover the remaining section 8 deadline-model items: one absolute deadline is carried across token acquisition, `401` refresh, and replay; the remaining budget is consumed across token acquisition, replay, and the outer retry backoff; and `connect-timeout` stays fixed on both resource and token clients rather than being attempt-clamped. The explicit TDD command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest com.digitalasset.canton.participant.extension.JdkHttpExtensionClientResourcesFactoryTest'`, and this slice went green immediately without production changes.
- 2026-04-01: Section 10 originally grouped signing-key and trust-material initialization into shared checklist items. That was too coarse once implementation reached the signing-key lifecycle slice, so those items are now split into separate signing-key and trust-material behaviors. The split is intentional and required by the maintenance rule that materially different behaviors stay independently checkable.
- 2026-04-01: `HttpExtensionOAuthClientAssertionFactoryTest` and `HttpExtensionServiceClientOAuthTest` now cover the signing-key lifecycle items in section 10: the service client does not touch the signing key during construction, missing keys fail only on the first OAuth call before any outbound HTTP, and a successfully loaded key is reused across later token reacquisition without rereading the key file. The explicit TDD command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionOAuthClientAssertionFactoryTest com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, and this slice went green immediately without production changes.
- 2026-04-01: Section 10 also originally collapsed all resource/token trust behavior into one broad “trust configuration independence” item. That is now split so the already-covered `tls-insecure` independence can be tracked separately from the still-open custom trust-material implementation.
- 2026-04-01: `JdkHttpExtensionClientResourcesFactoryTest` now also covers the remaining transport-ownership and `tls-insecure` independence items in section 10: each extension gets its own resource/token transports, resource and token `tls-insecure` settings are independent, and top-level resource `tls-insecure` does not bleed into the token client. The explicit TDD command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.JdkHttpExtensionClientResourcesFactoryTest'`, and this slice went green immediately without production changes.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the first log-redaction items in section 11 by capturing the full DEBUG-and-above log stream for a successful OAuth call and asserting that it does not contain the opaque access token, the client assertion, or token-endpoint request-body fields such as `grant_type` or `client_assertion`. The explicit TDD command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, and this slice went green immediately without production changes. The private-key-material log item remains open because it needs a stronger dedicated proof than this generic redaction check.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now covers the remaining section 11 logging items: token acquisition start, success, failure, cache reuse, expiry-driven reacquisition, `401`-driven invalidation, final failure classification, and private-key-material redaction during key-loading failure. The explicit TDD command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`: the first red exposed missing OAuth lifecycle log messages, a follow-up rerun exposed one compile issue where the new invalidation log needed an implicit `TraceContext`, and the slice went green after adding narrow OAuth token-state logging to `HttpExtensionServiceClient`.
- 2026-04-01: `JdkHttpExtensionClientResourcesFactoryTest` now also covers the remaining section 10 custom-trust behavior: resource and token transports can each use endpoint-specific trust material, the two trust settings remain independent under OAuth, and omitting custom trust falls back to the JVM default trust path rather than silently inheriting the other endpoint's trust settings. The explicit TDD command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.JdkHttpExtensionClientResourcesFactoryTest'`: the first red was the expected `SSLHandshakeException` on the new positive custom-trust examples because the JDK client builder still ignored `trust-collection-file`, and the suite went green after `JdkHttpExtensionClientResourcesFactory` started propagating endpoint-specific trust files into a custom SSL context built from the configured PEM trust collection.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the remaining section 10 construction-time trust-lifecycle items: invalid OAuth trust files do not fail `HttpExtensionServiceClient` construction, trust loading is deferred until the first OAuth call path, and that first-use failure is still mapped before any outbound HTTP with `requestId = None`. The explicit TDD command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, and this slice went green immediately without production changes because the existing lazy `resources` path already satisfied the new trust-laziness example.
- 2026-04-01: `HttpExtensionServiceClientOAuthTest` now also covers the remaining section 10 trust-lifetime items by running a real HTTPS token/resource server with custom trust files, deleting those trust files after the first successful call, expiring the token locally, and proving the second call still reacquires a token and reaches the resource server. The explicit TDD command remained `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`: the first red was test-side only because an inner helper `case class` triggered a Scala warning under `-Werror`, and the slice went green after changing that helper to a plain class. No production change was required; the existing lazy resources and long-lived JDK transports already satisfied the trust-reuse behavior.
- 2026-04-02: Reworked the external-call integration harness in `ExternalCallIntegrationTestBase` so section 12 can run on the shared dev-protocol fixtures. The blocker-resolution red-green driver for the shared harness was `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.BasicExternalCallIntegrationTestH2 -- -z "execute a single external call and return the result"'`. It first failed with `FAILED_PRECONDITION/PARTICIPANT_IS_NOT_ACTIVE`, which was fixed by bootstrapping the synchronizer manually with `topologyChangeDelay = 0` under `SimClock`/`RemoteClock`, plus disabling extension startup validation in the harness so startup does not depend on extension reachability.
- 2026-04-02: The same basic integration red-green driver then exposed a product bug in the ledger-api submission path: submission failed with `External call result not available (status=500, extensionId=test-ext, functionId=echo)` because `StoreBackedCommandInterpreter` was not wired to `ExternalCallHandler`. The minimal production fix threaded an optional `ExternalCallHandler` through `LedgerApiServer`, `ApiServiceOwner`, `ApiServices`, and `StoreBackedCommandInterpreter` so submission can materialize the external-call result before replay. The same targeted command then passed.
- 2026-04-02: `OAuthExternalCallIntegrationTest` now covers HTTPS success for both resource and token endpoints, cached-token reuse across business requests, explicit custom-trust configuration, and successful submission-plus-validation execution under OAuth. The explicit TDD command was `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2'`: after the shared harness and submission-path fixes above, the only remaining red was test-side because the resource call count must account for both submission and validation execution on the confirming participant. The suite went green after correcting that expectation from `2` to `4` resource calls while keeping `verifyTokenCallCount(1)`.
- 2026-04-02: Reverified the touched external-call integration suites with `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.BasicExternalCallIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2'`, which passed with 6 tests across 2 suites.
- 2026-04-02: `OAuthExternalCallIntegrationTest` now also covers expiry-driven token reacquisition on the next business request. The explicit TDD command remained `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2'`: the first red was test-side because `SharedEnvironment` retained the previous example's warm cache, which made the new slice start with `0` token calls. After isolating the slice on its own extension ID, the second red exposed the real product bug: advancing the integration sim clock still left the token cache warm because the participant path was using `System.currentTimeMillis()` instead of the participant clock. The slice went green after wiring `ParticipantNode` to construct `ExtensionServiceManager` with a clock-backed `HttpExtensionClientRuntime`.
- 2026-04-02: `OAuthExternalCallIntegrationTest` now also covers a single resource-server `401` causing one token refresh and one exact replay. The explicit command remained `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2'`, and this slice went green immediately once it was isolated on its own extension ID: one business request produced `2` token calls and `3` resource calls (`401`, replay success, validation success).
- 2026-04-02: Reverified the touched external-call integration suites again with `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.BasicExternalCallIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2'`, which passed with 8 tests across 2 suites.
- 2026-04-02: `OAuthExternalCallIntegrationTest` now also covers malformed token responses surfacing as final `502` failures before any resource request is sent. The explicit TDD command remained `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2'`: every red in this slice was test-side, first because `CommandFailure.toString` drops the detailed engine error, then because `LogEntry.commandFailureMessage` only applies to the environment-level error entry and not the OAuth WARN entries emitted by `HttpExtensionServiceClient`. The slice went green after asserting against the rendered warning/error log text instead. The final observable contract is: `3` token-endpoint calls (`maxRetries = 2`), `0` resource calls, and surfaced log/error text containing both `Malformed OAuth token response` and `status=502`.
- 2026-04-02: `HttpExtensionOAuthTokenResponseParserTest`, `HttpExtensionOAuthTokenClientTest`, and `HttpExtensionServiceClientOAuthTest` now cover the standards-compliance change for omitted `expires_in`: parser and token client accept otherwise valid token responses without `expires_in`, malformed `expires_in` still surfaces as `502`, and the service client does not place unknown-lifetime tokens into the shared cross-request cache. The explicit command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionOAuthTokenResponseParserTest com.digitalasset.canton.participant.extension.HttpExtensionOAuthTokenClientTest com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`. The first red was test-side only because a helper default argument triggered `-Werror`; the suite went green after making the helper overload explicit.
- 2026-04-02: `OAuthExternalCallIntegrationTest` now also covers HTTPS execution when the token response omits `expires_in`, proving that the participant still succeeds but reacquires a token for each OAuth-protected business request instead of reusing a shared cached token across later requests. The explicit command was `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2 -- -z "without cross-request token reuse when expires_in is omitted"'`. The first red was test-side only because the initial expectation counted one token acquisition per submission rather than per OAuth-protected business request on the confirming participant; the slice went green after correcting that expectation from `2` to `4`.
- 2026-04-02: Reverified the touched external-call integration suites once more with `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.BasicExternalCallIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2'`, which passed with 9 tests across 2 suites.
- 2026-04-02: `HttpExtensionServiceClientOAuthTest` now also covers the regression where OAuth mode silently fell back to the resource transport if the resources bundle omitted `tokenTransport`. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest -- -z "dedicated token transport"'`: it first failed because the call still succeeded through the resource transport (`result.isLeft` was false), then passed after `HttpExtensionServiceClient` began failing fast with a local pre-outbound `500` when OAuth resources do not provide a dedicated token transport. Reverified with `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientTest com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, which passed with 61 tests across 2 suites.
- 2026-04-02: `OAuthExternalCallIntegrationTest` now also covers a retryable token-endpoint failure followed by success through the outer retry loop. The explicit TDD command remained `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2'`, and this slice went green immediately without production changes: one business request produced `2` token-endpoint calls (`503` with `Retry-After: 0`, then `200`) and `2` resource calls (submission plus validation) before succeeding.
- 2026-04-02: `OAuthExternalCallIntegrationTest` now also covers a terminal token-endpoint failure surfacing the preserved HTTP status. The explicit TDD command remained `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2'`, and this slice also went green immediately without production changes: a token-endpoint `403` produced `1` token call, `0` resource calls, and rendered log/error text containing both `status=403` and the token-endpoint body.
- 2026-04-02: `OAuthExternalCallIntegrationTest` now also covers first-use local OAuth material failures. The explicit TDD command remained `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2'`, and this slice went green immediately without production changes: one extension is configured with a missing key file and another with a missing trust file at participant startup, startup still succeeds, and the first business use of each extension fails with `status=500`, `requestId=none`, `0` token-endpoint calls, and `0` resource calls.
- 2026-04-02: `BasicExternalCallIntegrationTest` now also covers the non-OAuth separation item in section 12: a plain `auth.type = none` resource `401` remains terminal and is not replayed. The explicit TDD command was `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.BasicExternalCallIntegrationTestH2'`, and this slice went green immediately without production changes: the failed business request produced rendered log text with `status=401` and exactly `1` resource call.
- 2026-04-02: `OAuthExternalCallStartupIntegrationTest` now covers the startup-traffic item in section 12 by starting the mock HTTPS/token server before participant startup and asserting that startup plus extension-manager construction produce `0` token-endpoint calls and `0` resource calls. The explicit TDD command was `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupIntegrationTestH2'`: the first red was test-side because the `SharedEnvironment` example omitted its fixture parameter, the second red was also test-side because `-Werror` rejected an unused implicit fixture parameter, and the slice went green after switching that example to an unused non-implicit placeholder.
- 2026-04-02: `OAuthExternalCallStartupValidationIntegrationTest` now covers the remaining section 12 startup-validation-semantics item. The explicit TDD command was `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupValidationIntegrationTestH2'`: the first red was expectation-side because the new test assumed `validateExtensionsOnStartup = true` would trigger one `_health` resource validation call, but the current startup path produced `0` resource calls and `0` token-endpoint calls even with both startup-validation flags forced to `true`. The slice went green after updating the assertion to lock down that actual current behavior for OAuth: invalid OAuth-only key material does not introduce startup gating or startup HTTP traffic.
- 2026-04-02: `OAuthEchoModeExternalCallIntegrationTest` now covers the remaining section 12 `echoMode` item under OAuth. The explicit TDD command was `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthEchoModeExternalCallIntegrationTestH2'`, and this slice went green immediately without production changes: one business request completed successfully with `echoMode = true`, `0` token-endpoint calls, and `0` resource calls.
- 2026-04-02: The former section 12 item about insecure-TLS integration scaffolding has been replaced with the narrower observable contract the harness actually uses: explicit trust roots rather than `tls-insecure`. No new integration test was added for insecure TLS because the harness no longer relies on it; the existing green HTTPS integration coverage in `OAuthExternalCallIntegrationTest` and the checked explicit-trust item are the relevant proofs.
- 2026-04-02: Reverified the touched external-call integration suites with `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.BasicExternalCallIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthEchoModeExternalCallIntegrationTestH2'`, which passed with 16 tests across 4 suites.
- 2026-04-02: Reverified the final touched external-call integration suites with `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.BasicExternalCallIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupValidationIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthEchoModeExternalCallIntegrationTestH2'`, which passed with 17 tests across 5 suites.
- 2026-04-02: `HttpExtensionServiceClientOAuthTest` now covers the retry-budget regression in section 13. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest -- -z "retry with a clamped timeout when positive budget remains for another outer attempt"'`: the new example first failed because `HttpExtensionServiceClient` refused the retry with `Cannot retry: insufficient time remaining (1050ms)`, then passed after removing the full-attempt admission rule and reserving only enough budget for the next outer attempt to start with a positive clamped timeout. The same production change also updated the adjacent deadline example in `HttpExtensionServiceClientOAuthTest` so total-budget consumption now preserves the full 1s exponential backoff and clamps the final request to the remaining 200ms. Reverified with `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientTest com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`, which passed with 60 tests across 2 suites.
- 2026-04-02: Startup-related checklist items were rewritten to match the updated spec. Earlier green notes that locked down the old lazy-startup behavior remain as historical context only and are superseded by the new startup local-preflight and remote-validation contract.
- 2026-04-02: `HttpExtensionServiceClientOAuthTest` now covers the new startup-local-preflight contract in section 10. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest -- -z "startup local preflight"'`: it first failed because `startupLocalPreflight` did not exist, then passed after `HttpExtensionServiceClient` began forcing resource/token client construction and building one OAuth client assertion without sending HTTP.
- 2026-04-02: `ExtensionServiceManagerTest` now covers startup orchestration for always-fatal local preflight plus flag-controlled remote validation. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.ExtensionServiceManagerTest -- -z "startup"'`: it first failed because `initializeOnStartup` did not exist, then passed after `ExtensionServiceManager` began running local preflight unconditionally and applying `failOnExtensionValidationError` only to remote-validation results.
- 2026-04-02: `HttpExtensionRequestBuilderOAuthTest` and `HttpExtensionServiceClientOAuthTest` now cover auth-aware remote validation. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionRequestBuilderOAuthTest com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest -- -z "validation"'`: it first failed because validation requests could not carry a bearer token, then passed after `HttpExtensionRequestBuilder` and `HttpExtensionServiceClient.validateConfiguration()` were updated so OAuth startup validation performs token acquisition followed by an authenticated `_health` request and treats `401`/`403` as invalid.
- 2026-04-02: `ExtensionServiceConfigOAuthTest` now locks down the startup-policy default change. The explicit red-green command was `sbt 'community-participant/testOnly com.digitalasset.canton.participant.config.ExtensionServiceConfigOAuthTest -- -z "opt-in by default"'`: it first failed because `EngineExtensionsConfig.validateExtensionsOnStartup` still defaulted to `true`, then passed after the default was changed to `false` while leaving `failOnExtensionValidationError = true`.
- 2026-04-02: The real participant boot path now executes extension startup initialization through `ParticipantNode`, and the old first-use local-material-failure expectations in `OAuthExternalCallIntegrationTest` have been retired. That broader reverify was driven by `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.BasicExternalCallIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthEchoModeExternalCallIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupPreflightIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupValidationIntegrationTestH2'`: the first red aborted `OAuthExternalCallIntegrationTestH2` during participant startup because the suite still configured missing key/trust extensions under the old lazy-startup assumption, then went green after moving those expectations entirely into the new startup suites.
- 2026-04-02: `OAuthExternalCallStartupIntegrationTest`, `OAuthExternalCallStartupPreflightIntegrationTest`, and `OAuthExternalCallStartupValidationIntegrationTest` now cover the startup-specific integration contract in section 12. The explicit red-green command was `sbt 'community-app/testOnly com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupPreflightIntegrationTestH2 com.digitalasset.canton.integration.tests.externalcall.OAuthExternalCallStartupValidationIntegrationTestH2'`: the first reds were test-side, first because `CommandFailure` in the console layer does not expose detailed error text directly, then because isolated environments reused a single mock-server port until teardown. The slice went green after asserting against captured warning/error log entries and attaching per-environment mock-server teardown to the startup suites.
- 2026-03-30: `HttpExtensionServiceClientTest` covers the currently verified auth-none `401` behavior and terminal classification for resource `400`, `401`, `403`, and `404`.
- 2026-03-30: Reverified the touched OAuth helper and service-client suites, plus the existing auth-none `HttpExtensionServiceClientTest`, with `sbt 'community-participant/testOnly com.digitalasset.canton.participant.extension.HttpExtensionOAuthTokenRequestBuilderTest com.digitalasset.canton.participant.extension.HttpExtensionOAuthClientAssertionFactoryTest com.digitalasset.canton.participant.extension.HttpExtensionOAuthTokenResponseParserTest com.digitalasset.canton.participant.extension.HttpExtensionOAuthTokenClientTest com.digitalasset.canton.participant.extension.HttpExtensionServiceClientTest com.digitalasset.canton.participant.extension.HttpExtensionServiceClientOAuthTest'`.
- 2026-03-30: Verified with `sbt 'community-participant/testOnly com.digitalasset.canton.participant.config.ExtensionServiceConfigOAuthTest com.digitalasset.canton.participant.extension.HttpExtensionRequestBuilderOAuthTest com.digitalasset.canton.participant.extension.HttpExtensionServiceClientTest com.digitalasset.canton.participant.extension.ExtensionServiceManagerTest com.digitalasset.canton.participant.extension.JdkHttpExtensionClientResourcesFactoryTest com.digitalasset.canton.participant.extension.ExtensionServiceExternalCallHandlerTest'`.
