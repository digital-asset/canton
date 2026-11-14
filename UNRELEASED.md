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
| yes        | yes                   | yes*     | yes           |

- Ledger API:
  - `SYNC_SERVICE_BECAME_PASSIVE` is no longer reported, and `SUBMISSION_DURING_SHUTDOWN` uses the status code
    `UNAVAILABLE` instead of `ABORTED`.
- Admin API and console:
  - A participant that registers a synchronizer with explicit onboarding transactions does not push its authorized
    store to that synchronizer automatically.
- Protocol:
  - (*) Synchronizers on protocol version 37 are not affected. Protocol version 38 will be rolled out as a new protocol
    version.
- Configuration:
  - Sequencer drivers (such as CometBFT) are no longer supported.
  - The Docker images enable the HTTP readiness endpoint by default, and the `HEALTHCHECK` of the
    `canton-participant` image uses it.

## What's New

### Protocol version 38

#### New limits
NOTE: These limits are provisional on protocol version 38 and may still change before it is released.

The following topology limits are static synchronizer parameters (`synchronizerLimits.topologyLimits`) and are enforced from protocol version 38.
- `TopologyLimits.maxVettedPackagesPerParticipant` (default 10000) bounds the number of packages a participant can vet in a `VettedPackages` topology mapping.
  It applies to the `packages` field and to the deprecated `package_ids` field individually.
- `TopologyLimits.maxSequencersPerSynchronizer` (default 100) bounds the number of active and observer sequencers combined in a `SequencerSynchronizerState` topology mapping.
- `TopologyLimits.maxMediatorsPerGroup` (default 100) bounds the number of active and observer mediators combined in a `MediatorSynchronizerState` topology mapping, i.e. per mediator group.
- `TopologyLimits.maxSignaturesPerTransaction` (default 1000) bounds the number of signatures on a signed topology transaction, single and multi transaction signatures combined.
- `TopologyLimits.maxTransactionHashesPerMultiSignature` (default 1000) bounds the number of transaction hashes a multi transaction signature covers.
- `TopologyLimits.maxTransactionsPerBroadcast` (default 1000) bounds the number of signed topology transactions in a topology transactions broadcast.
  A node's `topology.broadcast-batch-size` must not exceed it.
- `TopologyLimits.maxEndpointsPerSequencerConnection` (default 10) bounds the number of endpoints in the sequencer connection of an `LsuSequencerConnectionSuccessor` topology mapping.
- `EncryptedMultipleViewsMessage.session_key_lookup` is now bounded by `TransactionProtocolLimits.maxRecipientsPerEnvelope` (default 10000).

The following existing transaction protocol limit now also bounds another field.
- `ExternalAuthorization.authentications` of a submission is bounded by `TransactionProtocolLimits.maxActAs` (default 1000) from the dev protocol version (`nextValidationBoundPV`); stable versions are unchanged.

### For application developers

#### `UpdateFormat.includeTransactionsAndReassignments`

A shorthand field `includeTransactionsAndReassignments` of type `TransactionFormat` has been added to the `UpdateFormat` (used e.g. in `GetUpdates`, `GetUpdatesPage`, `GetUpdateByOffset`, `GetUpdateById` and `GetUpdateByHash`). Setting it is equivalent to setting `UpdateFormat.includeTransactions` to the given transaction format and `UpdateFormat.includeReassignments` to its event format, i.e. both transactions and reassignments are included in the response, filtered by the same event format. It must not be combined with `UpdateFormat.includeTransactions` or `UpdateFormat.includeReassignments`; such requests are rejected with `INVALID_ARGUMENT`. The JSON API and the Java bindings have been extended accordingly. As an illustration, the following request returns both transactions and reassignments of `Alice`:

```
  POST http://localhost:7575/v2/updates
  Content-Type: application/json
  {
    "beginExclusive": 0,
    "updateFormat": {
      "includeTransactionsAndReassignments": {
        "eventFormat": {"filtersByParty": {"Alice::...": {}}, "verbose": false},
        "transactionShape": "TRANSACTION_SHAPE_ACS_DELTA"
      }
    }
  }
```

#### JSON Ledger API: cookie authentication for websocket endpoints

Websocket endpoints of the JSON Ledger API now accept the JWT in a cookie named `jwt.token`, as an alternative to
passing the token in the `Sec-WebSocket-Protocol` header for browser clients that cannot set the `Authorization` header.
The `Sec-WebSocket-Protocol` has a risk of being exposed in logs, because it is not redacted by default by e.g. network proxies.

The token is looked up in this order: `Authorization` header, `Sec-WebSocket-Protocol` header, `jwt.token` cookie.
Regular HTTP endpoints ignore the cookie. A cookie-authenticated client must still offer the `daml.ws.auth` subprotocol
(e.g. `new WebSocket(url, ["daml.ws.auth"])`), as the server selects it in the handshake response.

Cookie authentication is disabled by default. To enable it, the participant operator has to list the allowed web
application origins in `canton.participants.<participant-id>.http-ledger-api.websocket-config.cookie-auth-allowed-origins`,
for example `["https://app.example.com"]`. Entries must match the browser's `Origin` header exactly
(scheme, host and port, no path); `*` allows any origin and should only be used for development.
A cookie-authenticated handshake whose `Origin` header is missing, `null`, or not in the list is refused with
`403 Forbidden` and the new error code `JSON_API_COOKIE_AUTH_ORIGIN_NOT_ALLOWED`. This protects against
cross-site websocket hijacking.

### All node operators

### Participant node operators

#### Sequencer connection penalty mechanism
The sequencer connection pool now implements a penalty mechanism to prioritize sequencers that are healthy and responsive
when picking connections.
When adverse events are observed on a connection, a penalty is applied to the sequencer. When asked for connections,
the sequencer client's connection pool will sort the candidate sequencers according to their penalties and choose the lower ones, picking randomly among ties.
Penalties decay exponentially over time, so that a sequencer that was previously penalized can recover and be selected again.

The mechanism is configured with the new admin API field `sequencer_connection_penalty_config` of `SequencerConnections`
(console: `SequencerConnections.withSequencerConnectionPenaltyConfig`), which has the following fields:
- `enabled` (default true) controls whether sequencer penalties affect the selection of sequencer connections. When disabled,
  penalties are still recorded, but sequencers are picked randomly regardless of their penalties (this is equivalent to the previous behavior).
- `half_life` (default 1 minute) controls the half-life of the exponential decay of penalties.
- `decay_interval` (default 10 seconds) controls the interval at which the decay is applied to the current penalty values.
- `zero_threshold` (default 0.01) controls the threshold below which penalties are reset to 0, so that a penalized sequencer
  can recover after a bounded number of decays.

The time for a penalty to decay below the threshold is given by `log2(penalty / zero_threshold) * half_life`, rounded up to a multiple of `decay_interval`.
For the default values and a penalty of 1, this is 6min 40s.
Due to the exponential nature of the decay, a penalty of 2 will take 1 half-life longer than a penalty of 1, a penalty of 4 will take 2 half-lives longer, and so on.

The metric `daml.sequencer-client.sequencer-connection-pool.sequencer-penalty` is available to monitor the current penalty values of sequencers;
its `sequencer` label holds the sequencer's unique identifier.

Currently, the following events make use of this mechanism:
- A penalty of 1 is given to a sequencer when it refuses a submission request with `SEQUENCER_OVERLOADED`.

### Synchronizer operators

## Breaking changes

### Ledger API (application developers)

#### Shutdown and passive error codes

- The error code `SYNC_SERVICE_BECAME_PASSIVE` is no longer reported, because it was not used consistently. It is replaced by `SUBMISSION_DURING_SHUTDOWN` or `ABORTED_DUE_TO_SHUTDOWN`.
- The error code `SUBMISSION_DURING_SHUTDOWN` now uses the status code `UNAVAILABLE` instead of `ABORTED` to correctly reflect that the service is unavailable and the call can be retried.
- Clients that retry on `ABORTED` must also retry on `UNAVAILABLE`.

### All node operators

#### Docker images use the HTTP readiness endpoint

- The `canton-participant`, `canton-mediator`, and `canton-sequencer` Docker images now enable the HTTP readiness endpoint (`/health/ready` on port 5071) by default.
- The `HEALTHCHECK` of the `canton-participant` Docker image now uses the HTTP readiness endpoint (`/health/ready` on port 5071) instead of the gRPC health service.
  Readiness is now only reported once database migrations and storage initialization have completed, so after an upgrade with long-running migrations the container stays unhealthy for longer.
  Tooling that waits for the container to become healthy (e.g., `docker compose up --wait` or `condition: service_healthy`) may need longer timeouts.

#### Removal of support for Postgres 14

- Postgres 14 reaches end-of-life in November 2026, as announced in the 3.6.1 release notes. Canton no longer supports it, and by default nodes do not start against a Postgres 14 database.
- Upgrade the database to Postgres 15 or later before upgrading Canton.
- To start a node on Postgres 14 nevertheless, set `canton.parameters.non-standard-config = true`. This configuration is not supported.

### Participant node operators

#### Explicit onboarding transactions disable authorized store dispatching

A participant that registers a synchronizer with explicit onboarding transactions does not push its authorized store to that synchronizer automatically.
Changes to its identity for that synchronizer must be proposed in the synchronizer store.
The new field `disable_authorized_store_dispatching` of `SynchronizerConnectionConfig` in the admin API shows it.

### Synchronizer operators

#### Removal of support for sequencer drivers
- Sequencer drivers (such as CometBFT) are no longer supported.
- Reference sequencer is also no longer supported.

## Deprecations

<!-- The deprecated item, its replacement, and when it will be removed. -->

- Deprecated the `canton.participants.<participant>.parameters.ledger-api-server.indexer.batching-parallelism` configuration setting, which is no longer supported.
- The traffic enforcement endpoints deprecated in 3.7 (`TrafficService.PruneEvents`, the JSON endpoint `POST /v2/traffic/events/prune` and the console command `ledger_api.traffic.prune_events`) are now disabled by default, and calls fail with `DEPRECATED_API_DISABLED`. Use `TrafficService.Prune` instead.
  Setting `canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-37 = true` re-enables them until their removal in 3.9.

## Minor Improvements

- The `Request-Timeout` header of JSON Ledger API requests now also accepts a time unit, for example `15s`. Values without a time unit are still interpreted as milliseconds.
- Indexer pipeline now breaks up heavy transaction to evenly sized batches, where the 'size' is determined by the number of database operations.
- Reads and writes from the active contract journal now use a batch aggregator for efficiency.

### Ledger API

- Ledger API `UpdateService`: participant authorization topology events (`TopologyFormat.include_participant_authorization_events`) are now publicly available.
  Previously `ReadAs` rights for every listed party (or `ReadAsAnyParty` for an empty party list) were required.
  The authorization required by the transaction and reassignment formats of the same request is unchanged.
- When reassigning if no validation package override is provided pick the highest commonly vetted package version.
- `PartyManagementService.ListKnownParties`: subsequent pages return the topology state seen when the first page was
  requested. Page tokens issued by another participant are rejected.

### All node operators

- The console commands `keys.secret.download` and `keys.secret.download_to` no longer require the preview feature flag.
  Downloading a key pair from a node still requires `canton.parameters.enable-key-download = true` on that node.

### Participant node operators

- Previously `canton.remote-participants.<remoteParticipant>.token` was used for both ledger API and admin API commands in the Canton Console.
  Now, an optional `canton.remote-participants.<remoteParticipant>.admin-api-token` can be specified to use a different token for admin API
  commands in case that admin API auth differs from ledger API auth.
- With submission request amplification, a sequencer refuses a copy of a confirmation response whose aggregation was
  already delivered through another sequencer. The participant now logs this at INFO instead of a WARN
  "Failed to send responses", because the responses were sequenced.

### Synchronizer operators

## Security and Bug Fixes

<!--
Advisories first, then the other fixes as a bullet list, including dependency updates for security vulnerabilities
(e.g. "Dependency updates to address security vulnerabilities: jackson 2.22.4, gRPC 1.85").
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

- The JSON Ledger API HTTPS server now uses the `PKIX` trust manager and key manager instead of the legacy `SunX509` ones.
  `PKIX` performs full RFC 5280 certification path validation, so certificate chains that were previously accepted
  but violate constraints such as the name constraints of an intermediate CA are now rejected.
- Minor bugfix to synchronization of `participants.parties.enable` which always timed out on a network
  with more than 100 parties.
- ACS commitments tolerate when a party changes its hosting state on a participant from "actively hosted" to back "onboarding".
- Fixed a bug in `topology_util.sh` where `compute_canton_hash` intermittently produced a wrong hash, causing certificates from `prepare-cert.sh` to be rejected.
- `commitments.reinitialize_commitments` returns only after the reinitialization has finished. Previously it could
  return slightly earlier, so an immediately following call could be rejected with "Reinitialization is already
  scheduled or in progress". A failed reinitialization is now reported right away instead of after the timeout.
- When a participant disconnects from a synchronizer while a topology transaction broadcast is pending, the
  participant no longer immediately resubmits the pending and queued topology transactions to that synchronizer. They
  are submitted after reconnecting, as before.

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
