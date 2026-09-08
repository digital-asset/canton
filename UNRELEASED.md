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

### CantonBFT
- Added the duration of the "output" stage to the performance metrics.
- Reduced compute footprint of mempool stage
- Make speculative download during state transfer only download epochs that are below the target epoch
of the state transfer.
- Fix some onboarding issues with catch up.
- Filter out unordered events in the mempool that exceed their max_sequencing_time

### Minor Improvements
- gRPC flow control now defaults to automatic mode with an explicit initial window size of 1MB
  across all Canton gRPC connections, including CantonBFT P2P.
  In addition, only one among flow control window size (manual flow control) and initial flow control window size
  (automatic flow control) can be set at a time, otherwise a validation error is produced.
  This change removes the dependency on the underlying gRPC implementation's default flow control behavior,
  which can vary across versions, makes it easier to troubleshoot flow control issues in the future and
  prevents configuration mistakes.

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

