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
- Added AWS KMS support for `Ed25519` signing keys like GCP KMS.
- Fixed creation of Ed25519 keys in GCP KMS to use only the software protection level, because they are not available with HSM protection level.
- `canton.features.enable-repair-commands = false` (default) now fully disables the repair service on the participant node.
- Upgraded bouncycastle in line with blackduck security recommendation.

### Preview Features
- preview feature

## Bugfixes
- Fixed slow query tracker mostly reporting `TryUtil.tryCatchInterrupted` instead of the actual originating call. 

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
