# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### Removal of protocol version 34
Protocol version 34 is not supported anymore. This release supports protocol versions 35, 36 and 37.

### Topic A
Template for a bigger topic
#### Background
#### Specific Changes
#### Impact and Migration

### Initialization changes for sequencers and mediators

Rather than genenerating the onboarding topology transactions during node initialization and storing these in the authorized store,
sequencers and mediators now skip this step and generate them when needed.

This ensures these topology transactions are generated in the right protocol version (which is not known during initialization).

Practically, this means that instead of the `node.topology.transactions.identity_transactions()` console command,
the `node.topology.transactions.generate_onboarding_transactions(protocolVersion)` command should be used instead.

If you are using an offline root key, you cannot call this
(but you would have already been signing and providing the transactions manually using the offline key).

### Key management operations are now bound to a synchronizer

The following console commands now take an optional `synchronizerId` argument:

- `node.topology.owner_to_key_mappings.add_key`
- `node.topology.owner_to_key_mappings.add_keys`
- `node.topology.owner_to_key_mappings.remove_key`
- `node.topology.owner_to_key_mappings.rotate_keys`
- `node.keys.secret.rotate_kms_node_key`
- `node.keys.secret.rotate_node_key`
- `node.keys.secret.rotate_node_keys`

If omitted, we will try to auto-detect the synchronizer instead.
This will only succeed when there is exactly a single registered and connected synchronizer.

It is considered a security best practice to have a distinct key per synchronizer.

### Synchronizer record time to offset conversion ledger API endpoint

To make cross-participant requests easier an API endpoint that converts a pair of synchronizer id and record time into a participant offset. The new endpoint was added to state service.

Please keep in mind that synthetic synchronizer events, like repair related events, will be put on the same record time
as the last observed synchronized event at the time of creation of this synthetic event. That means that multiple
participant offsets can exist for a single record time. The newly added endpoint returns the first record time that
happened on a requested record time or the last offset before that record time if there are no offset on there requested
record time.

Daml choices can now make external calls: deterministic calls to extension services that the
participant operator configures under `canton.participants.<participant>.parameters.engine.extensions`
(see `ExtensionServiceConfig`). The submitting participant executes each call and records the
result in the transaction; confirming participants re-validate the recorded results against
their own extension service before approving, and disagreements are rejected and alarmed.
The feature is early access: it requires the Daml package to use LF 2.4 or later and the
synchronizer to run protocol version 36 or later. For externally signed transactions the
recorded results are part of the prepared transaction and covered by the signed transaction
hash (hashing scheme version 4, available from protocol version 36).

### Minor Improvements
- Party queries are now served from the `lapi_events_party_to_participant` table, which also acquired a new index on `party + participant_id + synchronizer_id + event_sequential_id`. This change allows faithful representation of changes to the multi-hosted parties. At the same time, the `lapi_party_entries` has now been dropped.
- participant_id label is added onto participant metrics
- To maintain ledger consistency when importing repair events, repair events are now rejected if the last persisted event was a topology event. If this happens, reconnect to the synchronizer to move the record time. If cannot reconnect, use the `forceRepairWhenTopologyTransactionAtLedgerEnd` flag. Using the force flag can corrupt the data in the system.
- Deprecated configuration settings: `canton.participants.<participant>.parameters.ledger-api-server.indexer.use-weighted-batching` and `canton.participants.<participant>.parameters.ledger-api-server.indexer.submission-batch-insertion-size`. These are no longer supported.
- Support for OTLP remote metrics reporting, including optional OAuth2 Client Credentials authentication
  ```
  canton.monitoring.metrics.reporters = [{
    type = otlp
      endpoint = "http://localhost:4317"
      protocol = grpc
      auth {
        type = oauth-client-credentials
        token-url = "http://localhost:8080/token"
        client-id = "test-client"
        client-secret = "test-secret"
      }
   }]
  ```
- Added the request type to the sequencer cap rejection message.
- The participant status report now tracks the health components of all connected synchronizers instead of only the
  last connected one. Each per-synchronizer component status carries the physical synchronizer id in a new `labels`
  field (`synchronizer` key) of `ComponentStatus` in the health admin API, and the `Components:` section of
  `participant.health.status` renders node-level components as before, followed by a `Synchronizers:` section with the
  component states per connected synchronizer. The status can also be restricted to a single synchronizer via
  `participant.health.status(synchronizerId)` (accepting a logical or physical synchronizer id), backed by a new
  optional `synchronizer_id` field on `ParticipantStatusRequest`.
- Removed the default `non-standard-config = true` from the docker images configuration as they are not required for the default configuration.
- Increased the log level of Ledger API Indexer initialization and termination errors to WARN, except known transient failures that stay on INFO

### Preview Features
- preview feature

## Bugfixes
- bump da-base-image to 1.0.14
- Minor performance fix: time proofs have now a max sequencing timeout of 2 minutes and they no longer create a
  performance regression during synchronizer catch-up. Furthermore, synchronous rejects were not properly
  cleaned up by the sequencer client immediately after the reject was processed, but relied on the timeout logic to pick
  up the requests.
- Fixed the transaction trace shown for unhandled exceptions: when the exception passed through
  a try-catch that did not handle it, the trace started at the choice enclosing the try-catch
  instead of the choice where the exception was thrown.

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
