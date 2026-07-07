# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### Topic A
Template for a bigger topic
#### Background
#### Specific Changes
#### Impact and Migration

### Minor Improvements
- Improved log trace correlation in the JSON Ledger API: package and health endpoints that previously logged with an empty trace context now propagate the caller's `TraceContext`.
- The JSON Ledger API endpoint `GET /v2/interactive-submission/preferred-package-version` is now marked as deprecated in the OpenAPI specification as well (previoulsy it was marked as deprecated only in the textual description). Use `POST /v2/interactive-submission/preferred-packages` instead. The endpoint remains functional and will be removed in Canton 3.6.
- Improved log trace correlation in the BFT ordering layer: `TraceContext` is now propagated through module lifecycle calls (`setModule`, `become`, `ready`) and across several modules (`AvailabilityModule`, `IssConsensusModule`, `IssSegmentModule`, `PreIssConsensusModule`, `SegmentClosingBehaviour`, `P2PNetworkOutModule`, `StateTransferBehavior`) instead of using an empty trace context.

#### Improvements to `TopologyManagerReadService` and `TopologyAggregationService`
- The services return the error `TOPOLOGY_STORE_NOT_INITIALIZED` in case the the topology store hasn't been initialized yet.
  This can happen in two cases:
  - When onboarding to a synchronizer, before the download and the processing of the initial topology state has completed,
  - During a logical synchronizer upgrade, before the the successor store has been initialized either through the local topology copy or the download of the topology state from the successor sequencers.
- The services return the error `TOPOLOGY_STORE_NOT_FOUND` when requesting topology data for a synchronizer that is not known to the node.
- The services now only return data for all active synchronizers when no synchronizers are specified in the request.
  Previously, data for all synchronizers was returned, including the inactive predecessor synchronizers after an LSU.

#### Improvements to database connection error reporting
- If a database call takes more time than the configured `postgres_data_source.network_timeout` a new, more specific `INDEX_DB_SQL_NETWORK_TIMEOUT_ERROR` will be logged. This replaces the previously raised `INDEX_DB_SQL_NON_TRANSIENT_ERROR`.

### Preview Features
- preview feature

## Bugfixes

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


## What's Coming

We are currently working on

