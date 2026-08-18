# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### CantonBFT Improvements

- Added a configuration option to override the view change timeout that is set via a topology transaction (SequencingParameters)
- Improved the DB query that loads in-progress consensus state after a restart. Now, nodes only load minimum necessary messages for rehydration, rather than all messages from the most recent epoch.
- Add check in mempool that local batches created are not too big.
- CantonBFT: further fine tune what messages get loaded as part of rehydration

### Minor Improvements

- Updated Docker base image to 1.0.12, which updates gRPC health probe to v0.4.54.

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

