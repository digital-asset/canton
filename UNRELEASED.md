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
- Fixed an edge case that could cause deserialization of proto durations to fail
- Fixed an issue where topology transactions with max serial where not handled properly

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

## Bugfixes
- Ledger JSON API `/v2/state/active-contracts-page` is now available via POST; the GET variant that expects a request body is deprecated.


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

