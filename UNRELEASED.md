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

- Onboarding party submission prevention: Ensures a participant does not submit a transaction or reassignment on behalf
  of an onboarding party.
- OpenAPI and AsyncAPI files are now included in the API archive, and the bundle is published as a Maven artifact on
  GAR.
- `canton-protobuf.zip` has been renamed to `canton-api.zip` to reflect that the archive now contains more than protobuf files.
- New metric to track the number of active stakeholder groups of a participant: `daml.participant.sync.commitments.active-stakeholder-groups`
- *BREAKING*: ACS commitment metrics distinguish the synchronizer alias and distinguished counterparticipants via a label instead of including it in the metric name. Affected are the following metrics:
    - `daml.participant.sync.commitments.<synchronizer-alias>.counter-participant-latency.<participant>` -> `daml.participant.sync.commitments.counter-participant-latency`
    - `daml.participant.sync.commitments.<synchronizer-alias>.largest-counter-participant-latency` -> `daml.participant.sync.commitments.largest-counter-participant-latency`
    - `daml.participant.sync.commitments.<synchronizer-alias>.largest-distinguished-counter-participant-latency` -> `daml.participant.sync.commitments.largest-distinguished-counter-participant-latency`
- `<canton-node>.replication.connection-pool.connection.client-connection-check-interval` is introduced
  that allows configuring the PostgreSQL-specific `client_connection_check_interval` parameter for DB locked connections.
  This is a safety mechanism to prevent hanging connections in case of network issues. The default value is 5 seconds.
- GCP KMS configuration gains a new `key-version-overrides` map that lets operators specify a custom
  cryptoKey version per cryptoKey id (e.g. `key-version-overrides = { "my-imported-key" = "3" }`).
  Previously, Canton always used version `"1"`. Keys not listed in the map continue to default to
  version `"1"`, so existing configurations are unchanged. This is useful for cryptoKeys whose key
  material was [imported into GCP KMS](https://cloud.google.com/kms/docs/importing-a-key), where each
  import creates a new cryptoKey version and multiple versions can be relevant even for asymmetric
  keys.

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
