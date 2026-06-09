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
- Config flag `canton.participants.participant.parameter.alpha-multi-synchronizer-support` was renamed to `enable-all-ledger-api-reassignments`.
- Participant topology feature flag `EnableAlphaMultiSynchronizer` was renamed to `EnableMultiSynchronizer`.
- Improved coordination between the ACS commitment processor and its store, preventing benign `DB_STORAGE_DEGRADATION` warnings during participant shutdown.

#### Improvements around listing synchronizers

- Message `ListRegisteredSynchronizersRequest` accepts a new `all_statuses` attribute to allow to retrieve all
  registered synchronizers (and not only the active ones).
- Added new console command `participant1.synchronizers.list_all_registered` to list all registered synchronizers.

### Preview Features
- preview feature

## Bugfixes

### (YY-nnn, Severity): Title

#### Issue Description

#### Affected Deployments

#### Affected Versions

#### Impact

#### Symptom

#### Workaround

#### Likeliness

#### Recommendation

### Other bug fixes

- Fixed an issue that was making `ModifySynchronizer` request to fail if the physical synchronizer id was set.

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

