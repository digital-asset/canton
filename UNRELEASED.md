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
* Logging improvements in sequencer (around event signaller and sequencer reader).
* Canton startup logging: it is now possible to configure a startup log level, that will reset after a timeout, i.e.:
```hocon
canton.monitoring.logging.startup {
  log-level = "DEBUG"
  reset-after = "5 minutes"
}
```
* Sequencer progress supervisor: it is possible now to enable a monitor for the sequencer node progressing on its own subscription.
  * False positives related to asynchronous writer has been fixed
  * Added a warn action to kill the sequencer node
  * Configuration:
```hocon
// Future supervision has to be enabled
canton.monitoring.logging.log-slow-futures = true
canton.sequencers.<sequencer>.parameters.progress-supervisor {
  enabled = true
  warn-action = enable-debug-logging // set to "restart-sequencer" for sequencer node to exit when stuck
  // detection timetout has been bumped in defaults to
  // stuck-detection-timeout = 15 minutes
}
```
* Additional sequencer metrics:
  *  more `daml.sequencer.block.stream-element-count` metric values with `flow` labels from Pekko streams in the sequencer reader
  * new `daml.sequencer.public-api.subscription-last-timestamp` metric with the last timestamp read via the member's subscription,
  labeled by the `subscriber`
  * 2 new metrics to monitor the time interval covered by the events buffer `daml.sequencer.head_timestamp` and `daml.sequencer.last_timestamp`

* If the new connection pool is enabled, the health status of a node will present the following new components:
  * `sequencer-connection-pool`
  * `internal-sequencer-connection-<alias>` (one per defined sequencer connection)
  * `sequencer-subscription-pool`
  * `subscription-sequencer-connection-<alias>` (one per active susbcription)

* Sequencer nodes serving many validator subscriptions are not flooded anymore with tasks reading from the database.
  The parallelism is configured via the `sequencers.<sequencer>.parameters.batching.parallelism`. Notice that this
  config setting is used not just used for limiting the event reading, but elsewhere in the sequencer as well.

* Replaying of ACS changes for the ACS commitment processor has smaller memory overhead:
  * Changes are loaded in batches from the DB
  * ACS changes are potentially smaller because we remove activations and deactivations that cancel out.
  This is particularly useful for short-lived contracts

* New parameter `safeToPruneCommitmentState` in `ParticipantStoreConfig`, enabling to optionally specify
in which conditions counter-participants that have not sent matching commitments cannot block pruning on
the current participant. The parameter affects all pruning commands, including scheduled pruning.

* Additional metrics for the ACS commitment processor: `daml.participant.sync.commitments.last-incoming-received`, `daml.participant.sync.commitments.last-incoming-processed`, `daml.participant.sync.commitments.last-locally-completed`, and `daml.participant.sync.commitments.last-locally-checkpointed`.

* Extended the set of characters allowed in user-id in the ledger api to contain brackets: `()`.
  This also makes those characters accepted as part of the `sub` claims in JWT tokens.

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
