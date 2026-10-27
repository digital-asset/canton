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

### Minor Improvements
- Indexer pipeline now breaks up heavy transaction to evenly sized batches, where the 'size' is determined by the number of database operations.
- Deprecated the `canton.participants.<participant>.parameters.ledger-api-server.indexer.batching-parallelism` configuration setting, which is no longer supported.
- ConvertRecordTimeToOffset API call was not detecting situation where the requested record time at which there were multiple updates and some of them were already pruned was returning earliest not pruned offset, now the request is rejected with `RECORD_TIME_NOT_FOUND` error code.
- The topology and offline root key scripts support topology transaction proto version 31 (used from protocol version 37) which is now the default.
  Pass the protocol version with `--protocol-version` to `prepare-cert.sh`, `interactive_submission.py`,
  `interactive_topology_example.py`, and `interactive_topology_example.sh` if the topology transactions should be generated with a lower proto version (e.g., when connected to a synchronizer running protocol version 36).
- `prepare-cert.sh` rejects `--intermediate-delegation` combined with `--root-delegation` or `--delegation-restrictions`.
- `repair.purge` rejects contracts that are reassigned away from the synchronizer, or skips them when `ignoreAlreadyPurged` is set.
- A participant that registers a synchronizer with explicit onboarding transactions does not push its authorized store to that synchronizer automatically.
  Changes to its identity for that synchronizer must be proposed in the synchronizer store.
  The new field `disable_authorized_store_dispatching` of `SynchronizerConnectionConfig` in the admin API shows it.
- gRPC server handler logs now carry the request's trace id when a client calls with an empty `TraceContext`, for example the sequencer connection pool's `Authenticate`, `Subscribe` and `DownloadTopologyStateForInit`. The client interceptor uses a default trace id when the caller supplied none, instead of sending no `traceparent`.
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
- Set autovacuum and autoanalyze settings on the tables pruned by Ledger API pruning. Vacuum and analyze now trigger on
  fixed row-count thresholds instead of a fraction of the table size, so large tables are vacuumed and analyzed more often.
- The JSON Ledger API HTTPS server now uses the `PKIX` trust manager and key manager instead of the legacy `SunX509` ones.
  `PKIX` performs full RFC 5280 certification path validation, so certificate chains that were previously accepted
  but violate constraints such as the name constraints of an intermediate CA are now rejected.

### Preview Features
- preview feature

## Bugfixes

- Minor bugfix to synchronization of `participants.parties.enable` which always timed out on a network
  with more than 100 parties.

### CantonBFT
- Fixed two security issues whereby malicious sequencers could respectively trigger overflows
  and corrupt in-memory state in other sequencers (Canton security advisory `GHSA-wg5x-47q6-4rhm`).
- Fixed a security issue that could have allowed a byzantine sequencer elected as new view leader
  to break consensus (Canton security advisory `GHSA-jp98-9j6j-8g8j`).

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
