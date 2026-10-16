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

### Minor Improvements
- Indexer pipeline now breaks up heavy transaction to evenly sized batches, where the 'size' is determined by the number of database operations.
- Deprecated the `canton.participants.<participant>.parameters.ledger-api-server.indexer.batching-parallelism` configuration setting, which is no longer supported.
- ConvertRecordTimeToOffset API call was not detecting situation where the requested record time at which there were multiple updates and some of them were already pruned was returning earliest not pruned offset, now the request is rejected with `RECORD_TIME_NOT_FOUND` error code.
- The offline root namespace key scripts support the topology transaction proto version 31, used from protocol version 37.
  `prepare-cert.sh` requires `--protocol-version`, the protocol version of the target synchronizer.
- `prepare-cert.sh` rejects `--intermediate-delegation` combined with `--root-delegation` or `--delegation-restrictions`.

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
