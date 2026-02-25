# Release of Canton 3.4.11

Canton 3.4.11 has been released on February 16, 2026.

## Summary

This release improves on performance, observability, and reliability.

As part of the upgrade for this patch release a DB migration is required and
performed by Canton.

## What’s New

### Free confirmation responses

A new traffic control parameter has been added: `freeConfirmationResponses`.
When set to true on a synchronizer where traffic control is enabled, confirmation responses will not cost traffic.
Defaults to `false`.

### New Topology Processing and Client Architecture

Topology processing has been significantly refactored. Before, it used to perform a large number of sequential database
lookups during the validation of a topology transaction. The new validation pre-fetches all data of a batch into a
write through cache and only persists the data at the end of the batch processing. This means that where before
a batch of 100 txs needed 200-300 db round trips to process, this is now reduced down to effectively 2 db operations
(one batch read, one batch write).

In addition, the same write-through cache is now also leveraged for read processing, where the topology state is built
from the cache directly, avoiding further database round trips.

The new components can be turned on and controlled using

```
canton.<type>.<name>.topology = {
       use-new-processor = true // can be used without new client
       use-new-client = true // can only be used with new processor
       // optional flags
       enable-topology-state-cache-consistency-checks = true // enable in the beginning for additional consistency checks for further validation
       topology-state-cache-eviction-threshold = 250 // what is the oversize threshold that must be reached before we start eviction
       max-topology-state-cache-items = 10000 // how many items to keep in cache (uid, transaction_type)
}
```
The topology state cache will also expose appropriate cache metrics using the label "topology".

### Performance Improvements

* Fixed the private store cache to prevent an excessive number of database reads.

* Sequencer nodes serving many validator subscriptions are not overloaded anymore with tasks reading from the database.
  The parallelism is configured via the `sequencers.<sequencer>.parameters.batching.parallelism`. Notice that this
  config setting is used not just used for limiting the event reading, but elsewhere in the sequencer as well.

* Replaying of ACS changes for the ACS commitment processor has smaller memory overhead:
  * Changes are loaded in batches from the DB
  * ACS changes are potentially smaller because we remove activations and deactivations that cancel out.
  This is particularly useful for short-lived contracts

* Additional DB indices on the ACS commitment tables to improve performance of commitment pruning. This requires a DB migration.

* Added a mode for the mediator to process events asynchronously. This is enabled by default.
  In the new asynchronous mode, events for the same request id are processed sequentially, but events for different request ids are processed in parallel.
  The asynchronous mode can be turned off using `canton.mediators.<mediator-name>.mediator.asynchronous-processing = false`.

* The mediator now batches fetching from and storing of finalized responses. The batch behavior can be configured via the following parameters:
```hocon
canton.mediators.<mediator>.parameters.batching {
  mediator-fetch-finalized-responses-aggregator {
    maximum-in-flight = 2 // default
    maximum-batch-size = 500 // default
  }
  mediator-store-finalized-responses-aggregator {
    maximum-in-flight = 2 // default
    maximum-batch-size = 500 // default
  }
}
```

* New participant config flag `canton.participants.<participant>.parameters.commitment-asynchronous-initialization` to enable asynchronous initialization of the ACS commitment processor. This speeds up synchronizer connection if the participant manages active contracts for a large number of different stakeholder groups, at the expense of additional memory and DB load.

* Disabled last-error-log by default due to performance reasons. You can re-enable the previous behavior by passing `--log-last-errors=true` to the canton binary.

### Observability Improvements

* Additional sequencer metrics:
  * more `daml.sequencer.block.stream-element-count` metric values with `flow` labels from Pekko streams in the sequencer reader
  * new `daml.sequencer.public-api.subscription-last-timestamp` metric with the last timestamp read via the member's subscription,
  labeled by the `subscriber`
  * 2 new metrics to monitor the time interval covered by the events buffer `daml.sequencer.head_timestamp` and `daml.sequencer.last_timestamp`

* Additional metrics for the ACS commitment processor: `daml.participant.sync.commitments.last-incoming-received`, `daml.participant.sync.commitments.last-incoming-processed`, `daml.participant.sync.commitments.last-locally-completed`, and `daml.participant.sync.commitments.last-locally-checkpointed`.

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

* If the new connection pool is enabled, the health status of a node will present the following new components:
  * `sequencer-connection-pool`
  * `internal-sequencer-connection-<alias>` (one per defined sequencer connection)
  * `sequencer-subscription-pool`
  * `subscription-sequencer-connection-<alias>` (one per active susbcription)

* The acknowledgement metric `daml_sequencer_block_acknowledgments_micros` is now monotonic within restarts and ignores late/delayed member's acknowledgements.

* Mediator metric `daml_mediator_requests` includes confirmation requests that are rejected due to reusing the request UUID. Such requests are labelled with `duplicate_request -> true` on the metric.

### Other Minor Improvements

* Added an RPC and corresponding console command on the sequencer's admin API to
  generate an authentication token for a member for testing: `sequencer1.authentication.generate_authentication_token(participant1)`.
  Requires the following config: `canton.features.enable-testing-commands = yes`.

* Added a console command to logout a member using their token on a sequencer: `sequencer1.authentication.logout(token)`

* Added support for adding table settings for PostgreSQL. One can use a repeatable migration (Flyway feature) in a file
  provided to Canton externally.
    * Use the new config `repeatable-migrations-paths` under the `canton.<node_type>.<node>.storage.parameters` configuration section.
    * The config takes a list of directories where repeatable migration files must be placed, paths must be prefixed with `filesystem:` for Flyway to recognize them.
    * Example: `canton.sequencers.sequencer1.storage.parameters.repeatable-migrations-paths = ["filesystem:community/common/src/test/resources/test_table_settings"]`.
    * Only repeatable migrations are allowed in these directories: files with names starting with `R__` and ending with `.sql`.
    * The files cannot be removed once added, but they can be modified (unlike the `V__` versioned schema migrations), and if modified these will be reapplied on each Canton startup.
    * The files are applied in lexicographical order.
    * Example use case: adding `autovacuum_*` settings to existing tables.
    * Only add idempotent changes in repeatable migrations.

* KMS operations are now retried on HTTP/2 INTERNAL gRPC exceptions.

* New parameter `safeToPruneCommitmentState` in `ParticipantStoreConfig`, enabling to optionally specify
in which conditions counter-participants that have not sent matching commitments cannot block pruning on
the current participant. The parameter affects all pruning commands, including scheduled pruning.

* Extended the set of characters allowed in user-id in the ledger api to contain brackets: `()`.
  This also makes those characters accepted as part of the `sub` claims in JWT tokens.

* Set aggressive TCP keepalive settings on Postgres connections to allow quick HA failover in case of stuck DB connections.

* New configuration value for setting the sequencer in-flight aggregation query interval: `batching-config.in-flight-aggregation-query-interval`.

* Fixed an issue that could prevent the `SequencerAggregator` to perform a timely shutdown.

* Update to bouncy castle to 1.83 which fixes CVE-2024-29857 and CVE-2024-34447

## Compatibility

The following Canton protocol versions are supported:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Canton protocol versions   | 34          |

Canton has been tested against the following versions of its dependencies:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Java Runtime               | OpenJDK 64-Bit Server VM (build 21.0.5+1-nixos, mixed mode, sharing)               |
| Postgres                   | Recommended: PostgreSQL 17.8 (Debian 17.8-1.pgdg13+1) – Also tested: PostgreSQL 14.21 (Debian 14.21-1.pgdg13+1), PostgreSQL 15.16 (Debian 15.16-1.pgdg13+1), PostgreSQL 16.12 (Debian 16.12-1.pgdg13+1)           |
