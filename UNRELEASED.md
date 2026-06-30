# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### Post Quantum Cryptography

Added experimental support for ML-DSA. Currently only ML-DSA-65 is supported. Experimental algorithms need to be explicitly enabled via a node's crypto configuration in `<node>.crypto.enable-experimental = true`.

### Topic A
Template for a bigger topic
#### Background
#### Specific Changes
#### Impact and Migration

### Reassignment store database migration

On upgrade, a participant database migration updates the reassignment store: it backfills the new
`stakeholders` column from the persisted contracts and drops the old `contracts` column. This runs
automatically as part of the startup migration step. Its duration scales with the number of
reassignments in the store, taking roughly 1 second per 10,000 reassignments.

### Lookup by Transaction Hash (Ledger API)

The Ledger API update service now exposes a `GetUpdateByHash` endpoint. Given a transaction hash, it returns the corresponding transaction if the caller has visibility over it. The hash is available on externally-signed (Interactive Submission Service) transactions.

- gRPC: `UpdateService.GetUpdateByHash`
- JSON API: `POST /v2/updates/update-by-hash`

### Minor Improvements
- The concurrency limit interceptor `ActiveRequestInterceptor` now caches rejection responses instead of generating a new error (and thereby filling the stack trace) for each
  rejected response, once the concurrency limit is filled.
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
- BREAKING: Removed the `protocolVersion` parameter from all `<node>.topology.<mapping>.list` console commands as it was not working properly.
- *BREAKING*: `kms-driver-api` and `kms-driver-testing` are now published to Maven Central, and will no longer be available in Artifactory.
- Connection pool metrics:
    - Add a `psid` label, populated if it is provided when connecting. This should be the case starting from the second connection to a synchronizer, or upon LSU.
    - Close the `connection-health` and `subscription-health` metrics associated to the `psid` when the pool is closed, instead of closing all the existing ones when the pool is started.
- Updated com.google.protobuf libs from 3.25.5 --> 3.25.9
- A call to `AcknowledgeSigned` with a timestamp before the upgrade time returns immediately, without any acknowledgement being done.
- (Potentially) *BREAKING*: Aggregatable submissions are now rejected eagerly to preserve bandwidth.
  This means that the submission error code `SEQUENCER_AGGREGATE_SUBMISSION_ALREADY_SENT` may now also
  be returned during the synchronous submission of the sequencer, as the state of the aggregation is also
  checked before ordering. In addition, the GRPC error code has been modified from `FAILED_PRECONDITION` to
  `ALREADY_EXISTS` to better reflect the nature of the error. Clients should be updated to handle this error
  code accordingly. Due to backwards compatibility, the old GRPC error code will be returned for PV35 and
  before on the async path, and the new capability must only be turned on when all nodes have been
  upgraded to a Canton version that supports this change. The new capability can be enabled using `canton.sequencers.seq.parameters.enable-reject-delivered-aggregations-on-pv-35 = MED`
  for mediators. This can be combined with the new configuration option of the mediator `canton.mediators.mymediator.parameters.delayed-verdict-sender.enabled = true`.
  Generally, the sequencer will send out the verdict after reaching the threshold. All subsequent sent verdicts are thrown away. The new option now allows threshold + extra verdicts to be sent immediately, while the rest of the mediators will wait a short amount of time. This allows to reduce the load on the sequencer by 30%, creating more capacity for other transactions.
- The `ActAsAnyParty` access right has been added to the Ledger API. This allows a user to submit transactions on behalf
  of any party. This feature is intended for use cases where a user needs to act on behalf of multiple parties, such as
  in a multi-tenant environment. The new access right can be granted to the users only by a participant administrator
  either at user creation time or through the `GrantAccessRight` command.
- *BREAKING*: Unknown config keys are now making config parsing failing. This mechanism was already in place, but it didn't include all the config keys, which is now fixed.
- *BREAKING*: Separated the config for support of dev and alpha protocol versions. In order to use pv=dev, you now have to specify `dev-version-support = true` instead of `alpha-version-support = true` (`canton.parameters.non-standard-config = true` is still needed).
- The JSON Ledger API now rejects malformed identifiers (e.g. template-id) provided in requests (one that does not follow the
  `<package>:<moduleName>:<entityName>` format) with a descriptive `400 Bad Request` error that names the
  expected format, instead of surfacing it as an internal error.
- The default size of the Ledger API in-memory fan-out buffer (`<participant>.ledger-api.index-service.max-transactions-in-memory-fan-out-buffer-size`) has been increased from 1000 to 1100 to accommodate serving ACS commitments from the buffer.
- Ledger API and Indexer metrics clean-up:
  - *BREAKING*: The indexer queue metrics `daml.participant.api.indexer.indexer_queue_blocked`, `daml.participant.api.indexer.indexer_queue_buffered` and `daml.participant.api.indexer.indexer_queue_uncommitted` are now exposed as gauges instead of meters.
  - *BREAKING*: The metric `daml.participant.api.services.pruning.contract_pruning_retried`, which tracks how many times contract pruning was retried, is now a histogram.
  - *BREAKING*: Removed the unused metrics `daml.participant.api.lapi.streams.transaction_trees_sent` and `daml.participant.api.index.transaction_trees_buffer_size`.
- The `ledger-api-server-parameters.contract-id-seeding` configuration parameter is deprecated and no longer used. The contract ID seeding now uses the same random source as the rest of Canton, which is the equivalent of the `strong` type.

### Preview Features
- preview feature

## Bugfixes


#### Issue Description

#### Affected Deployments

#### Affected Versions

#### Impact

#### Symptom

#### Workaround

#### Likeliness

#### Recommendation

## Deprecations
- `StaticSynchronizerParameters.defaultsWithoutKMS` has been deprecated in favor of `StaticSynchronizerParameters.defaults`. Supported cryptographic schemes now have parity between KMS and non-KMS configurations.

### Reminder: Support for scope-based access tokens will be removed in version 3.7.
- "Scope-based" access tokens, i.e. JWTs without any audience specified, have been deprecated in version 3.5.
- Versions 3.5 and 3.6 allow configurations using the default or explicitly configured target audiences, and log a warning for non-compliant configurations.
- In release 3.7, support for "scope-based" tokens will be removed entirely to enforce a valid `aud` field in every incoming JWT.
  The `scope` field will be repurposed to serve exclusively as an additional, optional claim for fine-grained permissions.

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
