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

### Active Contracts Head Snapshot (ACHS)
The Active Contracts Head Snapshot (ACHS) is a new optional feature that maintains a continuously updated snapshot of
the currently active contracts. When enabled, the ACHS accelerates `GetActiveContracts` (ACS) queries by allowing them
to read directly from a pre-computed snapshot rather than scanning the full event log to reconstruct the active set.

ACHS is disabled by default. To enable it, configure the `achs-config` block under the participant's indexer settings:
```
canton.participants.<participant>.parameters.ledger-api-server.indexer.achs-config {
    valid-at-distance-target = 1000000
    last-populated-distance-target = 500000
}
```

The `valid-at-distance-target` controls how far behind the ledger end (in event sequential IDs) the snapshot's validity
point is maintained. The ACHS is not used for serving queries below its validity point, logging at INFO level "ACHS for <filter>
skipped since validAt (...) already surpassed requested activeAt (...)". If the `valid-at-distance-target`
value is too small, long-running ACS queries may observe the ACHS validity point
moving (mid-stream) past their requested offset, causing the stream to fall back to the slower filter tables query, logging
at INFO level "ACHS stream for <filter> fell back to filter tables from (...) since validAt (...) surpassed activeAtEventSeqId (...)". If
the value is too large, the tail portion of the ACS (between the ACHS validity point and the requested offset) must be
resolved from the filter tables, making that last segment more expensive.

As described above, when the ACHS validity point moves or is past the requested offset, an info-level log message is
emitted indicating that the stream fell back to the filter tables.
Two corresponding metrics, `achs_skips` and `achs_midstream_fallbacks`, are available under `daml.participant.api.index`
to help operators monitor the frequency of these fallbacks and tune the `valid-at-distance-target` accordingly.

The `last-populated-distance-target` controls the additional lag (in event
sequential IDs) for the population of ACHS in order to store only the long-lived contracts. A larger value reduces
database I/O by skipping short-lived contracts that are created and archived before they would be added to the snapshot.
However, setting it too large increases the cost of the remaining ACS tail, as more data must be fetched from the filter
tables to cover the gap between the last populated point and the ACHS validity point.

Further tuning parameters include:
- `population-parallelism`: number of parallel threads for adding activations to the ACHS during normal operation.
- `removal-parallelism`: number of parallel threads for removing deactivated activations from the ACHS during normal operation.
- `aggregation-threshold`: minimum batch size (in event sequential IDs) before ACHS maintenance work is emitted.
- `init-parallelism`: number of parallel threads for ACHS population and removal during initialization.
- `init-aggregation-threshold`: minimum batch size (in event sequential IDs) for ACHS maintenance during initialization.
- `buffer-size`: size of the internal buffer between the indexer pipeline and the ACHS maintenance flow.

The `deactivation_distances` histogram metric which is available under `daml.participant.api.indexer.deactivation_distances`
can help operators understand the distribution of contract lifetimes (the event sequential ID distance between a contract's
activation and its deactivation) and set an appropriate `last-populated-distance-target`. Ideally, the population distance
should be large enough so that most short-lived contracts are already deactivated and thus not added to the snapshot.

Three gauge metrics are available under `daml.participant.api.indexer` to monitor the ACHS state:
- `achs_valid_at`: the event sequential ID at which the ACHS is currently valid. ACS queries with a requested offset
  at or after this value can read directly from the ACHS.
- `achs_last_populated`: the last event sequential ID for which activations were added to the ACHS.
- `achs_last_removed`: the last event sequential ID for which deactivations were looked up and the corresponding
  activations were removed from the ACHS.

### Minor Improvements

- Onboarding party submission prevention: Ensures a participant does not submit a transaction or reassignment on behalf
  of an onboarding party.
- New metric to track the number of active stakeholder groups of a participant: `daml.participant.sync.commitments.active-stakeholder-groups`
- *BREAKING*: ACS commitment metrics distinguish the synchronizer alias and distinguished counterparticipants via a label instead of including it in the metric name. Affected are the following metrics:
    - `daml.participant.sync.commitments.<synchronizer-alias>.counter-participant-latency.<participant>` -> `daml.participant.sync.commitments.counter-participant-latency`
    - `daml.participant.sync.commitments.<synchronizer-alias>.largest-counter-participant-latency` -> `daml.participant.sync.commitments.largest-counter-participant-latency`
    - `daml.participant.sync.commitments.<synchronizer-alias>.largest-distinguished-counter-participant-latency` -> `daml.participant.sync.commitments.largest-distinguished-counter-participant-latency`
- `<canton-node>.replication.connection-pool.connection.client-connection-check-interval` is introduced
  that allows configuring the PostgreSQL-specific `client_connection_check_interval` parameter for DB locked connections.
  This is a safety mechanism to prevent hanging connections in case of network issues. The default value is 5 seconds.

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

