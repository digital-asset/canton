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
- improvement

### Preview Features
- preview feature

## Bugfixes

- CantonBFT: peer-to-peer networking does not support gRPC load balancing nor health-checking because the ordering
  protocol itself provides resilience, however they were mistakenly enabled and caused confusing log messages. This
  has been fixed by disabling them.
- Added additional validation of the request timestamp in confirmation response processing

- ACS commitment processor reinitialization will now use a smaller memory footprint due to a leaner loading of contract-ids.

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

