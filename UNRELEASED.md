# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### CantonBFT
- Batch fetching: retry patience defaults are now shorter to avoid long delays in case a batch is not retrieved timely.
- Improved flow control defaults to avoid slowing down catch-up.

### Topic A
Template for a bigger topic
#### Background
#### Specific Changes
#### Impact and Migration

### Minor Improvements
- Added runbook for reinitializing commitments to a recent point in time without reading or processing past changes.

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

