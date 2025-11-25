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
- Batching configuration now allows setting different parallelism for pruning (currently only for Sequencer pruning):
  New option `canton.sequencers.sequencer.parameters.batching.pruning-parallelism` (defaults to `2`) can be used
  separately from the general `canton.sequencers.sequencer.parameters.batching.parallelism` setting.

### Preview Features
- preview feature

## Bugfixes

### (25-012, Low): Ledger prune call erroneously emits an UNSAFE_TO_PRUNE error

#### Issue Description

Pruning bug that wrongly issues errors, but does not delete data that is not safe to prune. When no offset is safe to prune, i.e., the first unsafe offset is 1 (the first ledger offset), Canton pruning indicates in logs that it is safe to prune up to ledger end, which is wrong. However, the prune call subsequently logs an UNSAFE_TO_PRUNE error and the pruning call fails. Note that this is not a safety bug: validation checks still prevent Canton from actually pruning unsafe data.

#### Affected Deployments

Participant nodes

#### Affected Versions

All 3.x < 3.4.9

#### Impact

Minor: An error is logged when calling prune, which may alert an operator.

#### Symptom

Pruning logs that it is safe to prune up to some offset, but the pruning call fails when it tries to prune at that offset.

#### Workaround

Calls to prune eventually succeed when the unsafe to prune boundary advances.

#### Likelihood

Possible

#### Recommendation

Upgrade to >= 3.4.9

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
