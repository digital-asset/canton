# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### Removal of support for sequencer drivers
- Sequencer drivers (such as CometBFT) are no longer supported. The reference sequencer driver is still being used in tests but will also be discontinued soon.

### Topic A
Template for a bigger topic
#### Background
#### Specific Changes
#### Impact and Migration

### Breaking: Addition of a flag controlling presence of reassignment events in the `StateService`.

#### Background

Incomplete reassignments (`IncompleteAssigned` and `IncompleteUnassigned` events) are only relevant
to clients that work with contracts reassigned between multiple synchronizers. Most clients only
need the active contracts. Until now, the snapshot always included these events. The new flag lets
clients opt in to incomplete reassignments only when they need them.

#### Specific Changes

A flag controlling presence of reassignment events has been added to the `GetActiveContractsRequest`
and `GetActiveContractsPageRequest` of the `StateService`. If the `includeIncompleteReassignments`
flag is not set, the reassignments are excluded from the response stream. JSON version of the api
has been also extended accordingly. For details see the OpenApi specification. As an illustration,
the following request for the `active-contracts-page` endpoint will return only the active contracts
and exclude any reassignments:

  ```
    POST http://localhost:6865/v2/state/active-contracts-page
    Content-Type: application/json
    {
      "activeAtOffset": 1000000,
      "eventFormat": {...},
      "maxPageSize": 100,
      "pageToken": "",
      "includeIncompleteReassignments": false
    }
  ```

The `GetActiveContracts` stream continuation token and the `GetActiveContractsPage` page token formats
have changed to take the new flag into account, so tokens issued by previous Canton versions are rejected
with `INVALID_CONTINUATION_TOKEN` and `INVALID_ACS_PAGE_TOKEN` respectively.

#### Impact and Migration

This change is backwards incompatible, as the default value of the new flag is `false`,
which means that reassignments are excluded from the response stream. The previous behavior
of including reassignments in the response stream can be restored by setting
the `includeIncompleteReassignments` flag to `true` in the request.
This change in behavior affects all early adopters of the Multi-Synchonizer Alpha feature.

### New limits
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

### JSON Ledger API: cookie authentication for websocket endpoints

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

### Sequencer connection penalty mechanism
The sequencer connection pool now implements a penalty mechanism to prioritize sequencers that are healthy and responsive
when picking connections.
When adverse events are observed on a connection, a penalty is applied to the sequencer. When asked for connections,
the sequencer client's connection pool will sort the candidate sequencers according to their penalties and choose the lower ones, picking randomly among ties.
Penalties decay exponentially over time, so that a sequencer that was previously penalized can recover and be selected again.

The mechanism is configured with the following connection parameters, via the new `sequencer_connection_penalty_config` field in `SequencerConnections`:
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

### Minor Improvements
- Indexer pipeline now breaks up heavy transaction to evenly sized batches, where the 'size' is determined by the number of database operations.
- Deprecated the `canton.participants.<participant>.parameters.ledger-api-server.indexer.batching-parallelism` configuration setting, which is no longer supported.
- ConvertRecordTimeToOffset API call was not detecting situation where the requested record time at which there were multiple updates and some of them were already pruned was returning earliest not pruned offset, now the request is rejected with `RECORD_TIME_NOT_FOUND` error code.
- `repair.purge` rejects contracts that are reassigned away from the synchronizer, or skips them when `ignoreAlreadyPurged` is set.
- A participant that registers a synchronizer with explicit onboarding transactions does not push its authorized store to that synchronizer automatically.
  Changes to its identity for that synchronizer must be proposed in the synchronizer store.
  The new field `disable_authorized_store_dispatching` of `SynchronizerConnectionConfig` in the admin API shows it.
- gRPC server handler logs now carry the request's trace id when a client calls with an empty `TraceContext`, for example the sequencer connection pool's `Authenticate`, `Subscribe` and `DownloadTopologyStateForInit`. The client interceptor uses a default trace id when the caller supplied none, instead of sending no `traceparent`.
- A shorthand field `includeTransactionsAndReassignments` of type `TransactionFormat` has been added to the `UpdateFormat` (used e.g. in `GetUpdates`, `GetUpdatesPage`, `GetUpdateByOffset`, `GetUpdateById` and `GetUpdateByHash`). Setting it is equivalent to setting `UpdateFormat.includeTransactions` to the given transaction format and `UpdateFormat.includeReassignments` to its event format, i.e. both transactions and reassignments are included in the response, filtered by the same event format. It must not be combined with `UpdateFormat.includeTransactions` or `UpdateFormat.includeReassignments`; such requests are rejected with `INVALID_ARGUMENT`. The JSON API and the Java bindings have been extended accordingly. Since the field is part of the `GetUpdatesPage` page token checksum, the page token version has been bumped and page tokens issued by previous versions are rejected. As an illustration, the following request returns both transactions and reassignments of `Alice`:
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
- Fixed documentation comments in Ledger API proto definiton that mentioned usage of package id. This feature was removed in 3.5
- Ledger API `UpdateService`: participant authorization topology events (`TopologyFormat.include_participant_authorization_events`) are now publicly available.
  Previously `ReadAs` rights for every listed party (or `ReadAsAnyParty` for an empty party list) were required.
  The authorization required by the transaction and reassignment formats of the same request is unchanged.
- The error code `SYNC_SERVICE_BECAME_PASSIVE` is no longer reported, because it was not used consistently. It is replaced by `SUBMISSION_DURING_SHUTDOWN` or `ABORTED_DUE_TO_SHUTDOWN`.
- The error code `SUBMISSION_DURING_SHUTDOWN` now uses the status code `UNAVAILABLE` instead of `ABORTED` to correctly reflect that the service is unavailable and the call can be retried.
- Update versions of jackson to address security vulnerabilities.
- Update versions of opentelemetry and opentelemetry-instrumentation to address security vulnerabilities.
- Support for the Zipkin trace exporter (`canton.monitoring.tracing.tracer.exporter.type=zipkin`) has been removed following its deprecation. Users should migrate to the OTLP exporter.
  See https://opentelemetry.io/blog/2025/deprecating-zipkin-exporters/ for details.
- The JSON Ledger API HTTPS server now uses the `PKIX` trust manager and key manager instead of the legacy `SunX509` ones.
  `PKIX` performs full RFC 5280 certification path validation, so certificate chains that were previously accepted
  but violate constraints such as the name constraints of an intermediate CA are now rejected.
- Bump da-base-image to 1.0.18
- The `canton-participant`, `canton-mediator`, and `canton-sequencer` Docker images now enable the HTTP readiness endpoint (`/health/ready` on port 5071) by default.
- The `HEALTHCHECK` of the `canton-participant` Docker image now uses the HTTP readiness endpoint (`/health/ready` on port 5071) instead of the gRPC health service.
  Readiness is now only reported once database migrations and storage initialization have completed, so after an upgrade with long-running migrations the container stays unhealthy for longer.
  Tooling that waits for the container to become healthy (e.g., `docker compose up --wait` or `condition: service_healthy`) may need longer timeouts.

### Preview Features
- preview feature

## Bugfixes

- Minor bugfix to synchronization of `participants.parties.enable` which always timed out on a network
  with more than 100 parties.
- ACS commitments tolerate when a party changes its hosting state on a participant from "actively hosted" to back "onboarding".

### CantonBFT

### (YY-nnn, Risk): Title

#### Issue Description

#### Affected Deployments

#### Affected Versions

#### Impact

#### Symptom

#### Workaround

#### Likeliness

#### Recommendation

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
