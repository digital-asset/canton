# External Call OAuth Review Findings

## Purpose

This document records the findings from the OAuth review performed on 2026-04-02 against:

- the implementation in `community/participant/src/main/scala/com/digitalasset/canton/participant/extension`
- the repo-local contract in `community/participant/EXTERNAL_CALL_OAUTH_TECH_SPEC.md`
- the relevant OAuth RFCs, primarily RFC 6749, RFC 6750, and RFC 7523

It exists so future sessions can distinguish:

- true implementation gaps
- intentional v1 scope restrictions
- findings that were discussed and explicitly resolved

## Maintenance Rules

This is a living document and MUST be kept up to date as work on the findings progresses.

Whenever a future change affects any finding listed here, update this document in the same change.
That includes:

- changing a finding's status
- recording a product or specification decision
- narrowing or broadening the scope of a finding
- landing a code fix
- deciding that a finding is accepted-by-design
- deciding that a finding should no longer be tracked

When updating this document:

- keep the summary table aligned with the detailed sections
- keep each finding's status current
- record the reason for any status change
- record the concrete code, spec, or test-plan documents changed as follow-up when relevant
- preserve prior decisions unless they have been explicitly changed
- add a dated note in the relevant finding section when the resolution history would otherwise become unclear

## Review Context

The reviewed implementation is not a general-purpose OAuth client or authorization server.
It is the outbound OAuth client used by the `external_call` extension.

Its intended v1 profile is:

- `grant_type = client_credentials`
- client authentication via `private_key_jwt`
- bearer access tokens
- one token endpoint and one resource endpoint per configured extension

Several findings below are interoperability restrictions relative to the broader OAuth RFCs, not
necessarily defects relative to the narrower v1 contract adopted in this repository.

## Summary

| Finding | Topic | Status |
| --- | --- | --- |
| 1 | Insecure TLS (`tls-insecure`) support under OAuth | Resolved by spec clarification |
| 2 | `expires_in` treated as mandatory in token responses | Resolved by conservative RFC-compatible fix |
| 3 | Client assertion `aud` fixed to token endpoint URI | Accepted v1 design constraint |
| 4 | Resource-server `401` replay does not inspect `WWW-Authenticate` | Open RFC 6750 mismatch |
| 5 | Token endpoint path forbids query strings | Open interoperability restriction |

## Finding 1: Insecure TLS Support Under OAuth

### Original Concern

The implementation allows `tls-insecure` / trust-all TLS behavior for OAuth resource and token
endpoints. The original review concern was that this looked inconsistent with the local OAuth tech
spec, which previously described insecure TLS as test-only scaffolding rather than part of the
supported contract.

Relevant implementation paths:

- `community/participant/src/main/scala/com/digitalasset/canton/participant/extension/HttpExtensionClientSupport.scala`
- `community/participant/src/main/scala/com/digitalasset/canton/participant/extension/ExtensionServiceManager.scala`

### Discussion Outcome

The intended product behavior is that non-production and test deployments may need to enable OAuth
without having a certificate chain trusted by the participant. In that setting, an explicit
`tls-insecure` escape hatch is acceptable as a compatibility option, provided it remains:

- opt-in
- non-production in intent
- non-canonical relative to the secure contract

That means the existence of the switch is not itself a bug.

### Decision

Finding 1 is **resolved by specification clarification, not by code change**.

The repo decision is:

- keep the implementation behavior as it is
- keep `tls-insecure` available as an explicit non-production compatibility mode
- keep the canonical secure OAuth contract based on normal certificate validation
- do not reopen this as an implementation defect unless product intent changes

### Follow-up Applied

The following repo-local documents were updated to reflect that decision:

- `community/participant/EXTERNAL_CALL_OAUTH_TECH_SPEC.md`
- `community/participant/EXTERNAL_CALL_OAUTH_TEST_PLAN.md`

The old phrasing that treated insecure TLS as mere test-only scaffolding was removed or narrowed so
it no longer contradicts the intended supported behavior.

### Remaining Note

This decision does **not** imply that insecure TLS is recommended for production. It only means the
feature is accepted as a deliberate non-production compatibility mode.

## Finding 2: `expires_in` Is Treated As Mandatory

### Original Observation

The token-response parser rejects responses that do not contain `expires_in`.

Relevant implementation path:

- `community/participant/src/main/scala/com/digitalasset/canton/participant/extension/HttpExtensionOAuthTokenResponseParser.scala`

The parser currently requires:

- `access_token`
- `token_type`
- `expires_in`

and maps a missing or malformed `expires_in` to a malformed token response (`502`).

### Why This Was Raised

Under RFC 6749, `expires_in` is RECOMMENDED rather than strictly required. Some authorization
servers document token lifetime out of band and omit the field from the token response.

### Resolution

On 2026-04-02, this finding was resolved by broadening the implementation in the most conservative
way that still follows the rest of the Canton codebase.

The repo decision was:

- accept otherwise valid token responses that omit `expires_in`
- keep shared cross-request token reuse tied to explicit locally known expiry
- do not introduce a new "cache until `401`" policy for unknown-lifetime tokens
- do not infer expiry by decoding access-token claims locally

That means the implementation now:

- accepts missing `expires_in` for RFC 6749 compatibility
- computes a local expiry instant only when `expires_in` is present
- places only known-expiry tokens into the shared cache
- does not reuse a token without `expires_in` across later business requests

This matches the strongest internal precedent outside `external_call`: Canton-managed token reuse
flows expect an explicit expiry before a token enters shared refresh/cache state.

### Follow-up Applied

The following implementation and repo-local documents were updated:

- `community/participant/src/main/scala/com/digitalasset/canton/participant/extension/HttpExtensionOAuthTokenResponseParser.scala`
- `community/participant/src/main/scala/com/digitalasset/canton/participant/extension/HttpExtensionServiceClient.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/HttpExtensionOAuthTokenResponseParserTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/HttpExtensionOAuthTokenClientTest.scala`
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/HttpExtensionServiceClientOAuthTest.scala`
- `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/OAuthExternalCallIntegrationTest.scala`
- `community/participant/EXTERNAL_CALL_OAUTH_TECH_SPEC.md`
- `community/participant/EXTERNAL_CALL_OAUTH_TEST_PLAN.md`

### Status

Resolved.

## Finding 3: Client Assertion `aud` Is Fixed To The Token Endpoint URI

### Observation

The `private_key_jwt` client assertion always uses the token endpoint URI as the `aud` claim, with
no configuration override.

Relevant implementation path:

- `community/participant/src/main/scala/com/digitalasset/canton/participant/extension/HttpExtensionOAuthClientAssertionFactory.scala`

Relevant config path:

- `community/participant/src/main/scala/com/digitalasset/canton/participant/config/ExtensionServiceConfig.scala`

### Why This Was Raised

RFC 7523 allows authorization servers to define the acceptable audience value. In practice, some
providers expect:

- the token endpoint URL
- the issuer URL
- another AS-defined string

### Current Interpretation

This is intentionally narrow in this repository's v1 design. The local tech spec explicitly says
that the token-endpoint URI is used both as:

- the HTTP target for token acquisition
- the `aud` claim in the client assertion

and that no separate audience override is supported in v1.

### Discussion Outcome

On 2026-04-02, this finding was reviewed again against repo-local precedent with upstream-friendliness
as the deciding criterion.

The strongest relevant precedent elsewhere in Canton is that configurable audience handling exists
mainly for inbound JWT validation, where Canton checks presented tokens against configured target
audience values. No stronger repo-local convention was found that would require outbound JWT emitters
to expose an audience override by default.

For outbound token generation, the current `external_call` implementation, tests, and local spec are
already aligned on a narrow v1 contract: client assertion `aud` equals the token endpoint URI and no
separate override is supported.

### Decision

Finding 3 remains **accepted v1 scope and should stay as implemented unless a concrete provider
interoperability requirement forces expansion**.

The repo decision is:

- keep `aud = <token-endpoint URI>` in the client assertion
- do not add an assertion-audience override in v1
- do not reopen this as a defect unless a real provider-compatibility need justifies broader scope

### Status

Accepted v1 design constraint. This is only a problem if broader provider interoperability becomes a
goal.

## Finding 4: Resource `401` Replay Is Challenge-Blind

### Observation

When OAuth is enabled, any resource-server `401` triggers token invalidation, token reacquisition,
and one replay attempt.

Relevant implementation path:

- `community/participant/src/main/scala/com/digitalasset/canton/participant/extension/HttpExtensionServiceClient.scala`

The implementation does not inspect:

- `WWW-Authenticate`
- bearer `error="invalid_token"`
- bearer `error="insufficient_scope"`

### Why This Was Raised

RFC 6750 defines bearer-token error signaling through the `WWW-Authenticate` challenge. A `401`
does not always mean “refresh the token and retry”; it can also indicate another authorization
problem or an application-specific unauthorized response.

### Current Interpretation

This is a real RFC 6750 mismatch. The current implementation uses a simplified rule:

- if OAuth is enabled and the resource response is `401`, attempt one token refresh and replay

That may be acceptable for tightly controlled extension/resource deployments, but it is broader than
the bearer-token challenge semantics defined in RFC 6750.

### Status

Open.

## Finding 5: Token Endpoint Path Forbids Query Strings

### Observation

The token endpoint is configured as separate `host`, `port`, and `path` fields, and the `path`
validation explicitly rejects query strings and fragments.

Relevant implementation path:

- `community/participant/src/main/scala/com/digitalasset/canton/participant/config/ExtensionServiceConfig.scala`

### Why This Was Raised

OAuth itself does not forbid token endpoint URIs from containing query components. A provider could
legitimately publish a token endpoint URI that includes fixed query parameters.

### Current Interpretation

This is another **interoperability restriction** rather than a generic correctness bug.

The local tech spec currently matches the implementation and explicitly forbids query strings and
fragments in the configured token endpoint path. So this is a consciously narrow configuration
model, not an accidental drift.

### Status

Open as a possible future interoperability expansion, but consistent with the current repo-local v1
contract.

## Practical Guidance For Future Sessions

- Do not reopen Finding 1 as an implementation bug unless the product decision about non-production
  `tls-insecure` support changes.
- Treat Finding 2 as resolved unless the repo later wants a broader unknown-lifetime token reuse
  policy.
- Treat Finding 5 as an interoperability restriction that needs a product decision before code
  changes.
- Treat Finding 3 as intentional v1 scope, not a bug, unless support for more OAuth providers is
  explicitly requested.
- Treat Finding 4 as the strongest remaining standards mismatch if strict RFC 6750 behavior becomes
  important.
