# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

<!--
Add each entry to the section and audience it affects. Write for application developers and node operators:
what changed, why it matters to them, and what to do. A bigger topic gets its own `####` heading with the
paragraphs Background, Specific Changes, and Impact and Migration as needed; small changes are bullets.
A breaking change also gets a bullet in the summary of breaking changes, and its column is set to `yes`.
Empty sections and these comments are removed when the release notes are polished.
-->

## Summary

_Write summary of release_

**Summary of breaking changes** (see [Breaking changes](#breaking-changes) for details and migration):

| Ledger API | Admin API and console | Protocol | Configuration |
| :--------- | :-------------------- | :------- | :------------ |
| yes        | no                    | no       | no            |

- Ledger API:
  - The JSON Ledger API no longer returns the details of security sensitive errors (internal errors, authentication
    and permission failures), matching the gRPC Ledger API.

## What's New

### New protocol version

- New transaction protocol limit `TransactionProtocolLimits.maxViewsPerMessage` (default 100 000) bounds the number of view trees
  (`EncryptedMultipleViewsMessage.view_trees`) and of view hashes (`EncryptedMultipleViewsMessage.view_hashes`) in an
  encrypted views message. It applies from the dev protocol version and is carried on `v33` of the proto; stable versions
  are unchanged and must keep the default. It may later be merged with `maxTransactionRootViews` and `maxTransactionSubViews`.

### For application developers

#### JWT Tokens Scoped to API areas

The Ledger API now restricts access to API areas based on the `scope` field in JWTs.

The `scope` field should be a space-separated list of restrictions.
The available API area restrictions are currently:

| Area       | Available restrictions                                              |
| :--------- | :------------------------------------------------------------------ |
| `base`     | `base:read`                                                         |
| `ledger`   | `ledger:read`, `ledger:act`, `ledger:execute`                       |
| `packages` | `packages:write`                                                    |
| `vetting`  | `vetting:write`                                                     |
| `parties`  | `parties:allocate`, `parties:self`, `parties:read`, `parties:write` |
| `users`    | `users:self`, `users:read`, `users:write`                           |
| `idps`     | `idps:read`, `idps:write`                                           |
| `time`     | `time:write`                                                        |
| `traffic`  | `traffic:self`, `traffic:read`, `traffic:write`                     |

Unrecognized restrictions are ignored.
If there are no recognized restrictions in the `scope` field, the token remains unrestricted,
which is backwards-compatible with previous releases.

Individual endpoints document what permissions are required to access them.
Many endpoints will still require additional party-level permissions, e.g. `CanExecuteAs(p)`.

These restrictions are set up so Participant admins, IDP admins and regular
users are still able to access the same endpoints of the API as in previous
releases in a backwards-compatible way.

When trying to access an unauthorized area of the API, a more precise error
message will be logged, indicating which restriction is missing.  For example,
for non-admins who try to upload a package, rather than the previous message:

> Claims do not authorize the use of administrative services.

The following message is now logged:

> Claims do not authorize packages:write methods of the Ledger API.

### All node operators

### Participant node operators

### Synchronizer operators

## Breaking changes

### Ledger API (application developers)

### All node operators

### Participant node operators

### Synchronizer operators

### Removal of deprecated features

<!-- Features deprecated in an earlier release that this release removes: the feature, the release that deprecated
it, and its replacement. -->

### Removal of protocol versions

<!-- Protocol versions no longer supported: the versions, and how to upgrade synchronizers still on them. -->

## Deprecations

<!-- The deprecated item, its replacement, and when it will be removed. -->

## Minor Improvements

### Ledger API

### All node operators

### Participant node operators

### Synchronizer operators

## Security Fixes

<!--
Advisories first, then the other security fixes as a bullet list, including dependency updates for security
vulnerabilities (e.g. "Dependency updates to address security vulnerabilities: jackson 2.22.4, gRPC 1.85").
Remove this section if there are no security fixes.
-->

<!--
Advisory for a bug fix with a severity, e.g. (25-012, Low). For a Critical or High security fix, only once the
fix is deployed on MainNet:

### (YY-nnn, Risk): Title

#### Issue Description

#### Affected Deployments

#### Affected Versions

#### Impact

#### Symptom

#### Workaround

#### Likeliness

#### Recommendation
-->

## Bug Fixes

<!-- The other bug fixes as a bullet list, not split by audience. -->

- The JSON Ledger API leaked the details of errors whose category requires redaction (internal errors, security
  alerts, authentication and permission failures, unimplemented operations) when they were raised inside the JSON
  API itself. They are now redacted as on the gRPC Ledger API, see
  [JSON Ledger API redacts security sensitive errors](#json-ledger-api-redacts-security-sensitive-errors).
- The JSON Ledger API now bounds the size of each inbound websocket message by its `max-inbound-message-size`
  (default 10 MiB), as it already did for HTTP request bodies. A message above the limit, including one split into
  many fragments, is rejected with the new error code `JSON_API_WEBSOCKET_MESSAGE_TOO_LARGE` before it is assembled
  in memory, and the connection is closed. Binary websocket messages, which no JSON Ledger API endpoint accepts,
  are rejected with the new error code `JSON_API_WEBSOCKET_BINARY_MESSAGE_NOT_SUPPORTED` without being read.
- The JSON Ledger API checks that the caller of `POST /v2/dars`, `POST /v2/dars/validate` and the legacy
  `POST /v2/packages` holds the admin claim for the participant before it reads the request body, and reads the body
  without blocking a thread. Previously, many slow or stalled DAR uploads, even unauthenticated ones, could occupy
  every thread of the JSON API and make it unresponsive.
- A DAR upload to the JSON Ledger API that is larger than `max-inbound-message-size` is rejected with HTTP status 413
  and the new error code `JSON_API_REQUEST_BODY_TOO_LARGE` instead of an internal error.

## Compatibility

The following Canton protocol versions are supported:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Canton protocol versions   | PROTOCOL_VERSIONS          |

Canton has been tested against the following versions of its dependencies:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Java Runtime               | JAVA_VERSION               |
| Postgres                   | POSTGRES_VERSION           |
