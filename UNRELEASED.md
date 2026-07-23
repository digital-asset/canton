# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### Post Quantum Cryptography

Added experimental support for ML-DSA. Currently only ML-DSA-65 is supported. Experimental algorithms need to be explicitly enabled via a node's crypto configuration in `<node>.crypto.enable-experimental = true`.

### New Sequencer Aggregator

Re-implemented the Sequencer Aggregator to be more resilient to misbehaving sequencers. Switching between the old and new implementation is controlled by the `sequencer-client.use-new-aggregator` configuration option, which defaults to `true`.
One of the improvements allows the aggregator to detect sequencers that provide an incorrect event after that event has already reached consensus with sufficiently many other sequencers. A cache of past processed events is kept for that purpose, whose size is controlled by the `sequencer-client.past-events-cache-size` configuration option (default: 1000).

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

### Completion Lookup by Transaction Hash (Ledger API)

The Ledger API command completion service now exposes a `GetCompletionByHash` endpoint. Given a transaction hash, it returns the accepted completion (if any) and recent rejected completions for that hash. The hash is available on externally-signed (Interactive Submission Service) transactions.

- gRPC: `CommandCompletionService.GetCompletionByHash`
- JSON API: `POST /v2/commands/completion-by-hash`

### Party JWTs (self-signed JWTs)

- The Ledger API now exposes a `GetJwks` endpoint. This can be used to obtain public keys for specific parties in JWK format.
  * gRPC: `JoseService.GetJwks`
  * JSON API: `GET /v2/jose/jwks/synchronizer/<synchronizer-id>/party/<party-id>`

### Minor Improvements
- The HTTP server for the Ledger JSON API is now explicitly configured with a maximum content length. A new config option `http-ledger-api.max-inbound-message-size` has been added. If not configured, the gRPC setting `ledger-api.max-inbound-message-size` will be used.
    Previously, an implicit limit of 8 MB was used, so this change should not affect existing configurations.
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
- GCP KMS configuration gains a new `key-version-overrides` map that lets operators specify a custom
  cryptoKey version per cryptoKey id (e.g. `key-version-overrides = { "my-imported-key" = "3" }`).
  Previously, Canton always used version `"1"`. Keys not listed in the map continue to default to
  version `"1"`, so existing configurations are unchanged. This is useful for cryptoKeys whose key
  material was [imported into GCP KMS](https://cloud.google.com/kms/docs/importing-a-key), where each
  import creates a new cryptoKey version and multiple versions can be relevant even for asymmetric
  keys.
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
- Health check service improvements:
  - `liveness` gRPC health check is now up and reporting as `SERVING` before the database migrations are performed (when used with `migrate-and-start = true`), to address the issue of k8s liveness probe failures during long-running migrations.
  - Mediators will report `readiness` `NOT_SERVING` when `liveness` is also `NOT_SERVING`, where previously it was possible for a mediator to report `readiness` `SERVING` while `liveness` was `NOT_SERVING`.
  - HTTP health checks now expose the `liveness` and `readiness`, under the URIs `/health/liveness` or `health/live` and `/health/readiness` or `/health/ready` endpoints, respectively. `/health` is still available for backward compatibility, mapping to `readiness`.
- Improved log trace correlation in the JSON Ledger API: package and health endpoints that previously logged with an empty trace context now propagate the caller's `TraceContext`.
- *BREAKING*: Removed the deprecated `GetPreferredPackageVersion` endpoint of the `InteractiveSubmissionService`. Clients should use `GetPreferredPackages` instead, which resolves the preferred packages for one or more package-name vetting requirements in a single call. This affects both the Ledger API (gRPC `InteractiveSubmissionService.GetPreferredPackageVersion`) and the Ledger JSON API (`GET /v2/interactive-submission/preferred-package-version`). The `GetPreferredPackages` endpoint (gRPC and `POST /v2/interactive-submission/preferred-packages`) is now considered stable.
- *BREAKING*: Updated the list of default cipher suites according to the current OWASP recommendations.

  The list of removed suites:
  - `TLS_DHE_RSA_WITH_AES_256_GCM_SHA384`
  - `TLS_DHE_RSA_WITH_AES_128_GCM_SHA256`
  - `TLS_DHE_RSA_WITH_AES_256_CBC_SHA256`
  - `TLS_DHE_RSA_WITH_AES_128_CBC_SHA256`
  - `TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384`
  - `TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256`

  The list of new suites:
    - `TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256`
    - `TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384`
    - `TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256`
    - `TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256`

  To use non-default cipher suites for backwards compatibility, set the following values in the config (not recommended):
  - `canton.participants.<participant>.ledger-api.tls.ciphers` if you have `canton.participants.<participant>.ledger-api.tls` set already
  - `canton.sequencers.<sequencer>.public-api.tls.ciphers` if you have `canton.sequencers.<sequencer>.public-api.tls` set already

  For full compatibility, set the entire old list of suites:
  ```
  canton.participants.<participant>.ledger-api.tls.ciphers =
    [
      "TLS_AES_256_GCM_SHA384",
      "TLS_CHACHA20_POLY1305_SHA256",
      "TLS_AES_128_GCM_SHA256",
      "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
      "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"
    ]
  ```

  ```
  canton.sequencers.<sequencer>.public-api.tls.ciphers =
    [
      "TLS_AES_256_GCM_SHA384",
      "TLS_CHACHA20_POLY1305_SHA256",
      "TLS_AES_128_GCM_SHA256",
      "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
      "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"
    ]
  ```
- AWS KMS keys created by Canton can now be configured with custom tags through the `custom-tags` setting.
- The default of the indexer's uncommitted-queue warning threshold (`<participant>.ledger-api.indexer.queue-uncommitted-warn-threshold`) has been increased from 5000 to 14000. This threshold controls when the `Uncommitted queue is growing too large` warning is emitted.
- Admin API streaming calls are now bounded to protect nodes from resource exhaustion:
  - Their duration is capped by the new `<node>.parameters.processing-timeouts.admin-stream-open-bound` value (default: 2 hours).
  - The number of concurrently open calls is capped per method (default: 10) via the admin server's `limits` configuration.
- *BREAKING*: The sequencers now also enforce confirmation throughput caps. If throughput caps are
  configured, they will also apply to confirmations. The cap for confirmations can be configured with `confirmation-response.per-client-tps-cap`.
  It should be set slightly above the global confirmation request cap, as a member should not need to confirm more
  requests than can be submitted globally. The `confirmation-response.global-tps-cap` can be set to the maximum
  sequencer event capacity.
- Changing identity providers for a party through `UpdatePartyDetails` is no longer possible, `UpdatePartyIdentityProviderId` must be used instead.
- An internal change is introduced which grants `ReadAsAnyParty` permissions to the traffic enforcement service. This removes the need to use non-standard config options to run local traffic enforcement with auth enabled:
  ```
  canton.participants.<participant_name>.ledger-api.admin-token-config.act-as-any-party-claim=true
  canton.participants.parameters.non-standard-config=true
  ```
- The participant admin party is now exempt from the traffic enforcement balance check during submission.
- Subview package vetting, checked at submission time, is now verified during confirmation request validation.

### Preview Features
- preview feature

## Bugfixes
- Ledger JSON API `/v2/state/active-contracts-page` is now available via POST; the GET variant that expects a request body is deprecated.

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
