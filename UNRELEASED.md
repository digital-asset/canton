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

### Minor Improvements
- Indexer pipeline now breaks up heavy transaction to evenly sized batches, where the 'size' is determined by the number of database operations.
- Deprecated the `canton.participants.<participant>.parameters.ledger-api-server.indexer.batching-parallelism` configuration setting, which is no longer supported.
- ConvertRecordTimeToOffset API call was not detecting situation where the requested record time at which there were multiple updates and some of them were already pruned was returning earliest not pruned offset, now the request is rejected with `RECORD_TIME_NOT_FOUND` error code.
- `prepare-cert.sh` of the offline root namespace key scripts requires `--protocol-version`, the protocol version of the target synchronizer.
- `prepare-cert.sh` rejects `--intermediate-delegation` combined with `--root-delegation` or `--delegation-restrictions`.
- A participant that registers a synchronizer with explicit onboarding transactions does not push its authorized store to that synchronizer.
  Changes to its identity for that synchronizer must be proposed in the synchronizer store.
  The new field `disable_authorized_store_dispatching` of `SynchronizerConnectionConfig` in the admin API shows it.
- gRPC server handler logs now carry the request's trace id when a client calls with an empty `TraceContext`, for example the sequencer connection pool's `Authenticate`, `Subscribe` and `DownloadTopologyStateForInit`. The client interceptor uses a default trace id when the caller supplied none, instead of sending no `traceparent`.
- Fixed documentation comments in Ledger API proto definiton that mentioned usage of package id. This feature was removed in 3.5

### Preview Features
- preview feature

## Bugfixes

### CantonBFT
- Fixed two security issues whereby malicious sequencers could respectively trigger overflows
  and corrupt in-memory state in other sequencers (Canton security advisory `GHSA-wg5x-47q6-4rhm`).
- Minor bugfix to synchronization of `participants.parties.enable` which always timed out on a network
  with more than 100 parties.

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
