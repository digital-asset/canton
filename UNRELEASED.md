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
- Added participant-side synchronizer connect debug logging to aid in diagnosing a rare reconnect hang.
- Improved the resilience of the CachedJwtVerifierLoader JWK cache during temporary JWKS endpoint outages.
  Set the `jwks-cache-config.auto-refresh-after` configuration to a positive duration below `jwks-cache-config.cache-expiration`
  on any participant, sequencer, and mediator node `admin-api`, participant node `ledger-api`, and sequencer node `public-api`
  to reduce the chances of cache misses and corresponding auth outages.

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

