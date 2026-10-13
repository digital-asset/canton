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

### Event sequential id range lookup of Ledger API update streams with index scans
#### Background
When serving an update stream, the Ledger API server first resolved the requested offset range into the
corresponding range of event sequential ids. This used to aggregate over all `lapi_update_meta`
rows in the requested offset range, so its cost grew with the size of the requested offset range.

#### Specific Changes
- The event sequential id range is now resolved with two index lookups on `lapi_update_meta`.

#### Impact and Migration
Starting an update stream over a large offset range is cheaper. Results returned by the Ledger API are unchanged and
no migration is needed.

### Minor Improvements
- improvement

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

