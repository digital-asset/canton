# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

<!--
Add each entry to the section and audience it affects. Write for application developers and node operators:
what changed, why it matters to them, and what to do. A bigger topic gets its own `####` heading with the
paragraphs Background, Specific Changes, and Impact and Migration as needed; small changes are bullets.
A breaking change also gets a bullet in the summary of breaking changes, and its column is set to `yes`.
Empty sections and these comments are removed when the release notes are polished.
-->

## Summary

_Write summary of release_

**Summary of breaking changes** (see [Breaking changes](#breaking-changes) for details and migration):

| Ledger API | Admin API and console | Protocol | Configuration |
| :--------- | :-------------------- | :------- | :------------ |
| yes        | yes                   | yes*     | yes           |

- Ledger API:
  - `SYNC_SERVICE_BECAME_PASSIVE` is no longer reported, and `SUBMISSION_DURING_SHUTDOWN` uses the status code
    `UNAVAILABLE` instead of `ABORTED`.
  - Scope-only JWTs, i.e. tokens with a `scope` claim but no audience, are rejected by default. The feature flag
    `canton.features.deprecated.enable-scope-only-jwt = true` re-enables them until their removal.
- Admin API and console:
  - A participant that registers a synchronizer with explicit onboarding transactions does not push its authorized
    store to that synchronizer automatically.
- Protocol:
  - (*) Synchronizers on protocol version 37 are not affected. Protocol version 38 will be rolled out as a new protocol
    version.
- Configuration:
  - An `auth-services` entry with a `target-scope` but no `target-audience` is rejected unless scope-only JWTs are
    re-enabled with the feature flag.
  - Sequencer drivers (such as CometBFT) are no longer supported.
  - The Docker images enable the HTTP readiness endpoint by default, and the `HEALTHCHECK` of the
    `canton-participant` image uses it.

## What's New

### Protocol version 38

#### Free ACS commitment messages

ACS commitment messages are no longer charged to the traffic account.

#### New collection size limits
NOTE: These limits are provisional on protocol version 38 and may still change before it is released.

Starting with protocol version 38, the following collections are bounded. Messages exceeding a limit are rejected.
The limits are static synchronizer parameters.

| Field                                                                                                                       | Default limit | How to configure                                                                                                      |
|-----------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------|
| Authentications of an externally signed submission (`ExternalAuthorization.authentications`)                                | 1000          | `synchronizer_limits.transaction_protocol_limits.max_act_as`                                                          |
| View trees in an encrypted views message (`view_trees`)                                                                     | 100 000       | `synchronizer_limits.transaction_protocol_limits.max_views_per_message`                                               |
| View hashes in an encrypted views message (`view_hashes`)                                                                   | 100 000       | `synchronizer_limits.transaction_protocol_limits.max_views_per_message`                                               |
| Entries of `session_key_lookup` in an encrypted views message                                                               | 10000         | `synchronizer_limits.transaction_protocol_limits.max_recipients_per_envelope`                                         |
| Each of `core_inputs`, `created_core`, `created_in_subview_archived_in_core` and `resolved_keys` of a `ViewParticipantData` | 100 000       | `synchronizer_limits.transaction_protocol_limits.max_contracts_per_view`                                              |
| `external_call_results` of a `ViewParticipantData`                                                                          | 10000         | `synchronizer_limits.transaction_protocol_limits.max_external_call_results_per_view`                                  |
| `checking_parties` of a `ViewExternalCallResult`                                                                            | 100 000       | `synchronizer_limits.transaction_protocol_limits.max_quorum_size`                                                     |
| Signatures in a `SignedContent`                                                                                             | 1             | Not configurable                                                                                                      |
| Packages in a `VettedPackages` mapping (`packages` and `package_ids` each)                                                  | 10000         | `synchronizer_limits.topology_limits.max_vetted_packages_per_participant`                                             |
| Active and observer mediators in a `MediatorSynchronizerState` mapping                                                      | 1000          | `synchronizer_limits.topology_limits.max_mediators_per_group`                                                         |
| Active and observer sequencers in a `SequencerSynchronizerState` mapping                                                    | 1000          | `synchronizer_limits.max_sequencers_per_synchronizer`                                                                 |
| Endpoints in a sequencer connection of an `LsuSequencerConnectionSuccessor` mapping                                         | 10            | `synchronizer_limits.topology_limits.max_endpoints_per_sequencer_connection`                                          |
| Signatures on a signed topology transaction                                                                                 | 1000          | `synchronizer_limits.topology_limits.max_signatures_per_transaction`                                                  |
| Transaction hashes in a multi transaction signature                                                                         | 1000          | `synchronizer_limits.topology_limits.max_transaction_hashes_per_multi_signature`                                      |
| Signed topology transactions in a broadcast                                                                                 | 1000          | `synchronizer_limits.topology_limits.max_transactions_per_broadcast`                                                  |
| Requests in a BFT ordering batch (`ordering_requests`)                                                                      | 32767         | `synchronizer_limits.bft_orderer_limits.max_ordering_requests`                                                        |
| Proofs of availability in a BFT ordering block proposal (`proofs`)                                                          | 32767         | `synchronizer_limits.bft_orderer_limits.max_proofs`                                                                   |
| Per-sequencer collections in BFT ordering messages (see below)                                                              | 1000          | `synchronizer_limits.max_sequencers_per_synchronizer`                                                                 |
| Per-epoch block collections in BFT ordering messages (see below)                                                            | 10 000 000    | `synchronizer_limits.bft_orderer_limits.max_segment_length` and `synchronizer_limits.max_sequencers_per_synchronizer` |

- The `max_quorum_size` default limit was reduced to 100 000
- `topology.broadcast-batch-size` of a node must not exceed the maximum number of signed topology transactions in a broadcast.
- Per-sequencer collections (segments, prepares, commits, acks, view changes, canonical commits and the view change,
  prepare and commit presence lists) are each bounded by `max_sequencers_per_synchronizer`.
- Per-epoch block collections (block statuses, blocks complete, consensus certificates, pre-prepares and commit
  certificates) are each bounded by `max_segment_length` (default 10 000) times `max_sequencers_per_synchronizer`.

### Rate limiting of gRPC requests

Canton now ships a gRPC server interceptor for rate limiting requests per gRPC method and per client IP, built on the
rate limiter ported from Splice in an earlier change. No server installs it and there is no configuration to turn it on,
so nothing is rate limited yet. Making it configurable on the sequencer public API follows in a later change.

#### Restricted synchronizers

The onboarding restriction for a synchronizer applies only to onboarding new participants:
- Already successfully onboarded participants retain access to the synchronizer and continue to host their parties, independent of whether they have an active participant synchronizer permission.
- Onboarded participants can update their synchronizer trust certificates also on restricted or locked synchronizers.
- In order to offboard a participant permanently, the synchronizer owners can revoke the participant's synchronizer trust certificate. For blocking the participant temporarily, the synchronizer owners can set `loginAfter` appropriately in the participant's synchronizer permission.

### For application developers

#### JWT Tokens Scoped to API areas

The Ledger API now restricts access to API areas based on the `scope` field in JWTs.

The `scope` field should be a space-separated list of restrictions.
The available API area restrictions are currently:

| Area       | Available restrictions                                              |
| :--------- | :------------------------------------------------------------------ |
| `base`     | `base:read`                                                         |
| `ledger`   | `ledger:read`, `ledger:act`, `ledger:execute`                       |
| `packages` | `packages:write`                                                    |
| `vetting`  | `vetting:write`                                                     |
| `parties`  | `parties:allocate`, `parties:self`, `parties:read`, `parties:write` |
| `users`    | `users:self`, `users:read`, `users:write`                           |
| `idps`     | `idps:read`, `idps:write`                                           |
| `time`     | `time:write`                                                        |
| `traffic`  | `traffic:self`, `traffic:read`, `traffic:write`                     |

Unrecognized restrictions are ignored.
If there are no recognized restrictions in the `scope` field, the token remains unrestricted,
which is backwards-compatible with previous releases.

Individual endpoints document what permissions are required to access them.
Many endpoints will still require additional party-level permissions, e.g. `CanExecuteAs(p)`.

These restrictions are set up so Participant admins, IDP admins and regular
users are still able to access the same endpoints of the API as in previous
releases in a backwards-compatible way.

When trying to access an unauthorized area of the API, a more precise error
message will be logged, indicating which restriction is missing.  For example,
for non-admins who try to upload a package, rather than the previous message:

> Claims do not authorize the use of administrative services.

The following message is now logged:

> Claims do not authorize packages:write methods of the Ledger API.

#### `UpdateFormat.includeTransactionsAndReassignments`

A shorthand field `includeTransactionsAndReassignments` of type `TransactionFormat` has been added to the `UpdateFormat` (used e.g. in `GetUpdates`, `GetUpdatesPage`, `GetUpdateByOffset`, `GetUpdateById` and `GetUpdateByHash`). Setting it is equivalent to setting `UpdateFormat.includeTransactions` to the given transaction format and `UpdateFormat.includeReassignments` to its event format, i.e. both transactions and reassignments are included in the response, filtered by the same event format. It must not be combined with `UpdateFormat.includeTransactions` or `UpdateFormat.includeReassignments`; such requests are rejected with `INVALID_ARGUMENT`. The JSON API and the Java bindings have been extended accordingly. As an illustration, the following request returns both transactions and reassignments of `Alice`:

```
  POST http://localhost:7575/v2/updates
  Content-Type: application/json
  {
    "beginExclusive": 0,
    "updateFormat": {
      "includeTransactionsAndReassignments": {
        "eventFormat": {"filtersByParty": {"Alice::...": {}}, "verbose": false},
        "transactionShape": "TRANSACTION_SHAPE_ACS_DELTA"
      }
    }
  }
```

#### JSON Ledger API: cookie authentication for websocket endpoints

Websocket endpoints of the JSON Ledger API now accept the JWT in a cookie named `jwt.token`, as an alternative to
passing the token in the `Sec-WebSocket-Protocol` header for browser clients that cannot set the `Authorization` header.
The `Sec-WebSocket-Protocol` has a risk of being exposed in logs, because it is not redacted by default by e.g. network proxies.

The token is looked up in this order: `Authorization` header, `Sec-WebSocket-Protocol` header, `jwt.token` cookie.
Regular HTTP endpoints ignore the cookie. A cookie-authenticated client must still offer the `daml.ws.auth` subprotocol
(e.g. `new WebSocket(url, ["daml.ws.auth"])`), as the server selects it in the handshake response.

Cookie authentication is disabled by default. To enable it, the participant operator has to list the allowed web
application origins in `canton.participants.<participant-id>.http-ledger-api.websocket-config.cookie-auth-allowed-origins`,
for example `["https://app.example.com"]`. Entries must match the browser's `Origin` header exactly
(scheme, host and port, no path); `*` allows any origin and should only be used for development.
A cookie-authenticated handshake whose `Origin` header is missing, `null`, or not in the list is refused with
`403 Forbidden` and the new error code `JSON_API_COOKIE_AUTH_ORIGIN_NOT_ALLOWED`. This protects against
cross-site websocket hijacking.

#### Ledger API: hosted parties snapshot

The Ledger API adds `GetHostedParties` (streaming) and `GetHostedPartiesPage` (paged) to query which participants
host parties on which synchronizers, with their permissions and hosting states. Applications can use this snapshot
to discover party hostings or initialize their topology view without replaying the full history of topology updates.

The snapshot is taken at `hosted_at_offset`, defaulting to the current ledger end for the initial request.
Continuation and page tokens preserve that offset. To keep the view up to date, stream topology updates with
`GetUpdates` starting from the snapshot's `hosted_at_offset`.

Omitting `filter` selects the caller's readable parties hosted on any synchronizer on the local participant.
`party_filter` and `synchronizer_id_filter` can be used to limit the results to specified parties and/or synchronizers.
Only local participant hostings are returned unless `all_participants` is true. Hosting information for other
participants may be incomplete. Hosting information is public, so the results are not restricted by the caller's
party read rights.

### All node operators

#### Fault-tolerant sequencer subscriptions
The sequencer subscription pool now manages subscription failures much more resiliently. While a failure on a single subscription
used to disconnect the node from the synchronizer, the pool now instead makes use of the penalty mechanism (see "Sequencer connection penalty
mechanism" below). Faulty sequencers get penalized, so that healthy ones are favored. The penalized sequencers are still available for
use when they recover, increasing the chances of keeping connectivity and making progress.
This behavior also applies to permission denied errors (e.g. after a node is off-boarded): the node stays connected and keeps retrying,
which allows it to automatically resume if its permissions are restored. To stop the retries, the operator must disconnect the node explicitly.

The sequencers that caused failures since the last successfully processed event are tracked:
- when their number reaches the trust threshold, a `SEQUENCER_TRUST_ASSUMPTION_VIOLATED` security alarm is logged; if there are enough
  remaining healthy sequencers, the node will continue processing events
- when their number is such that the remaining healthy sequencers cannot reach the trust threshold (for example: 4 sequencers, a trust
  threshold of 3, and 2 failures), a `SEQUENCER_UNREACHABLE_TRUST_THRESHOLD` error is logged and the sequencer client's health transitions
  to `Failed`; as long as this situation persists, the node will be unable to make progress, so a prompt intervention by an operator is
  required to restore operation

These situations may be due to a local problem (such as a corrupted database), or to faulty or malicious sequencers. In either case,
the node keeps retrying in the background and will automatically recover as soon as an event is successfully processed.

Major failures, such as the node's inability to process events, remain fatal and disconnect the node from the synchronizer:
- participants terminate if `exit-on-fatal-failures` is enabled (which is the default)
- mediators report their liveness as `NOT_SERVING`, allowing an external liveness probe to restart them

### Participant node operators

#### Sequencer connection penalty mechanism
The sequencer connection pool now implements a penalty mechanism to prioritize sequencers that are healthy and responsive
when picking connections.
When adverse events are observed on a connection, a penalty is applied to the sequencer. When asked for connections,
the sequencer client's connection pool will sort the candidate sequencers according to their penalties and choose the lower ones, picking randomly among ties.
Penalties decay exponentially over time, so that a sequencer that was previously penalized can recover and be selected again.

The mechanism is configured with the new admin API field `sequencer_connection_penalty_config` of `SequencerConnections`
(console: `SequencerConnections.withSequencerConnectionPenaltyConfig`), which has the following fields:
- `enabled` (default true) controls whether sequencer penalties affect the selection of sequencer connections. When disabled,
  penalties are still recorded, but sequencers are picked randomly regardless of their penalties (this is equivalent to the previous behavior).
- `half_life` (default 1 minute) controls the half-life of the exponential decay of penalties.
- `decay_interval` (default 10 seconds) controls the interval at which the decay is applied to the current penalty values.
- `zero_threshold` (default 0.01) controls the threshold below which penalties are reset to 0, so that a penalized sequencer
  can recover after a bounded number of decays.
- `penalty_values` controls the penalty values applied to a sequencer for each kind of adverse event. The default values are available in `PenaltyValues.default`.

The time for a penalty to decay below the threshold is given by `log2(penalty / zero_threshold) * half_life`, rounded up to a multiple of `decay_interval`.
For the default values and a penalty of 1, this is 6min 40s.
Due to the exponential nature of the decay, a penalty of 2 will take 1 half-life longer than a penalty of 1, a penalty of 4 will take 2 half-lives longer, and so on.

The metric `daml.sequencer-client.sequencer-connection-pool.sequencer-penalty` is available to monitor the current penalty values of sequencers;
its `sequencer` label holds the sequencer's unique identifier.

Currently, the following events make use of this mechanism:
- A penalty is given to a sequencer when it refuses a submission request with `SEQUENCER_OVERLOADED`.
- A penalty is given to a sequencer when its subscription fails; the value depends on the actual type of failure.

#### ACS commitment implementations

The new ACS commitment processor is now enabled always.
The config option `canton.participants.<participant>.parameters.acs-commitments.enable-new-acs-commitment-processor` is deprecated and ignored.
The old ACS commitment processor can only be enabled for protocol versions up to 37 inclusive;
for protocol versions 38 and above, the config option `canton.participants.<participant>.parameters.acs-commitments.disable-old-acs-commitment-processor` is ignored.

### Traffic enforcement

These changes apply to participants with traffic accounting enabled. The options named below are in `canton.participants.<participant>.traffic-accounting`.

#### Choosing the traffic account

- Command submissions, interactive submissions, and reassignment commands have a new optional `account_id` field that selects the traffic account to charge instead of the acting party.
  The console commands take a matching `accountId` parameter, and the Java bindings a `withAccountId` method.
- Setting `account_id` requires the new user right `CanDebitTrafficFrom(account_id)` or `CanDebitTrafficFromAnyAccount`, or an act-as or execute-as right for the party with that id.
  Only participant admins can grant the new rights.
- Account ids now have a fixed format (segments separated by `/`, at most 255 characters), which every party id meets. Requests for other ids are rejected.

#### Charging submissions

- The participant now registers every transaction and reassignment with the traffic enforcement app before sending it, and charges its completion to the registered account.
  If the app can't be reached, the submission is rejected unless `allow-submissions-on-degradation` is on.
- Without an `account_id`, a submission is charged to its acting party, as before. Transactions with several acting parties aren't charged (or are rejected with `reject-multi-party-submissions`), and automatic assignments are charged to the participant's admin party.
- New option `debit-rejected-completions` (default `true`) controls whether rejected completions are charged.

#### Balance checks against cost estimates

- With `enforce-cost-on-submissions` on, interactive preparation and transactions that need reassignments are checked against the estimated total cost, including reassignments, scaled by the new option `estimate-multiplier` (default `1.0`).
  The balance can become negative if the actual cost is higher.

#### Ledger API request size limits
The participant now bounds the size of the fields of Ledger API requests by:
- the number of elements of repeated fields (e.g. the commands of a submission)
- the number of entries of map fields (e.g. the `filters_by_party` of an event format)
- the size of bytes fields (e.g. the `dar_file` of an upload)
- the serialized size of selected fields (e.g. the Daml values of the commands).

A request that exceeds a limit is rejected with:
- `FIELD_CARDINALITY_LIMIT_EXCEEDED` for the number of elements of a repeated or map field
- `FIELD_SIZE_LIMIT_EXCEEDED` for the size of a bytes field or the serialized size of a field
- `EVENT_FORMAT_FILTERS_LIMIT_EXCEEDED` for the total number of filters of the event formats (see below).

The limits are configured under `canton.participants.<participant>.ledger-api.size-limits`, in four groups:
- `shared-size-config`: limits that apply to the same kind of field in several requests. For example,
  `dar-file-bytes-size-limit` applies to both `UploadDarFileRequest` and `ValidateDarFileRequest`.
- `public-lapi-size-config`, `admin-lapi-size-config` and `interactive-lapi-size-config`: the request-specific limits of
  the public services, the admin services and the interactive submission service. For example, `upload-dar-file-request`
  contains the configuration for the fields of `UploadDarFileRequest`.
  A request-specific limit that is set takes precedence over the shared limit it would otherwise inherit.
- Custom limits that are not tied to a single field are also under the limits configuration. For example,
  `event-format-total-filters-size-limit` bounds the filters of the event formats of a request after
  they are expanded across the requested parties. A template or wildcard filter counts as one and an interface filter as five.

All limits are still bounded by the gRPC maximum inbound message size of the Ledger API (`max-inbound-message-size`, default 10 MiB).

The exhaustive list of limits with their default values can be found in the documentation (TBD).

### Synchronizer operators

- Added a partial database index to support lookups of onboarding sequencer members.

## Breaking changes

### Ledger API (application developers)

#### Shutdown and passive error codes

- The error code `SYNC_SERVICE_BECAME_PASSIVE` is no longer reported, because it was not used consistently. It is replaced by `SUBMISSION_DURING_SHUTDOWN` or `ABORTED_DUE_TO_SHUTDOWN`.
- The error code `SUBMISSION_DURING_SHUTDOWN` now uses the status code `UNAVAILABLE` instead of `ABORTED` to correctly reflect that the service is unavailable and the call can be retried.
- Clients that retry on `ABORTED` must also retry on `UNAVAILABLE`.

### All node operators

#### Scope-only JWTs disabled by default

##### Background

"Scope-only" access tokens, i.e. JWTs that carry a `scope` claim but no audience, were deprecated in Canton 3.5
in favour of audience-based tokens, whose `aud` claim names the participant
(`https://daml.com/jwt/aud/participant/<participantId>`) or the explicitly configured `target-audience`.
Rather than removing them outright, this release disables them by default and puts them behind a feature flag,
in the same way as the Ledger API endpoints deprecated in 3.4 and 3.5.

##### Specific Changes

Scope-only JWTs are **disabled by default**:
- An `auth-services` entry with a `target-scope` but no `target-audience` is rejected when the configuration is
  validated, on the participant's Ledger API as well as on the Admin API of participants, sequencers and mediators.
- When neither `target-scope` nor `target-audience` is configured, a token without a participant audience that relied
  on the `daml_ledger_api` scope is now rejected with `UNAUTHENTICATED`. The rejection reason logged by the node names
  the feature flag that re-enables such tokens temporarily.
- Scope-only JWTs issued by an identity provider configured without an audience (`identity_provider_config.audience`)
  are also rejected.

They can be re-enabled temporarily with the feature flag that the errors name:

```
canton.features.deprecated.enable-scope-only-jwt = true
```

The flag is global, i.e. it applies to every node in the process, because the same authentication services run
on the Admin API of every node type. With the flag set, the previous behaviour is restored, and every authentication
service that accepts scope-only tokens, i.e. one with a `target-scope` or one without any target, logs a deprecation
warning at startup. The one-off warning that was logged when the first scope-only token was received has been removed.

##### Impact and Migration

Applications and identity providers that still issue scope-only tokens, and configurations that rely on a `target-scope`
alone, stop working and should be migrated to audience-based tokens. Alternatively, the feature flag can be set to
re-enable them, but be advised that doing so is only a temporary solution, as the tokens and the flag will be completely
removed in a future release. Downstream projects that build scope-only tokens for their tests are affected in the same way.

#### Superfluous Configuration Flags

Some expert batching and caching parameters were shared across node types but had no effect on
certain nodes. These unused settings are now deprecated and ignored on those nodes. For example,
`canton.mediators.mediator1.parameters.caching.sequencer-catchup-payload-cache` is ignored on
mediators. Remove deprecated settings from your configuration to avoid deprecation messages.
The previously deprecated `caching.package-dependency-cache` setting remains accepted and ignored.

#### Docker images use the HTTP readiness endpoint

- The `canton-participant`, `canton-mediator`, and `canton-sequencer` Docker images now enable the HTTP readiness endpoint (`/health/ready` on port 5071) by default.
- The `HEALTHCHECK` of the `canton-participant` Docker image now uses the HTTP readiness endpoint (`/health/ready` on port 5071) instead of the gRPC health service.
  Readiness is now only reported once database migrations and storage initialization have completed, so after an upgrade with long-running migrations the container stays unhealthy for longer.
  Tooling that waits for the container to become healthy (e.g., `docker compose up --wait` or `condition: service_healthy`) may need longer timeouts.
- JWKS-based token verification (the `jwt-jwks` auth service and identity provider configs) now caches the whole JWKS document per JWKS URL instead of individual keys.
  A token whose `kid` is not in the cached JWKS triggers a new fetch of the JWKS only if the last fetch attempt is older than the new `jwks-cache-config.min-refresh-interval` (default 30 seconds).
  As a consequence, a new signing key published by the identity provider is accepted at most `min-refresh-interval` after its first use.
  `jwks-cache-config.cache-max-size` now limits the number of cached JWKS URLs instead of the number of cached keys.

#### Removal of support for Postgres 14

- Postgres 14 reaches end-of-life in November 2026, as announced in the 3.6.1 release notes. Canton no longer supports it, and by default nodes do not start against a Postgres 14 database.
- Upgrade the database to Postgres 15 or later before upgrading Canton.
- To start a node on Postgres 14 nevertheless, set `canton.parameters.non-standard-config = true`. This configuration is not supported.

### Participant node operators

#### Explicit onboarding transactions disable authorized store dispatching

A participant that registers a synchronizer with explicit onboarding transactions does not push its authorized store to that synchronizer automatically.
Changes to its identity for that synchronizer must be proposed in the synchronizer store.
The new field `disable_authorized_store_dispatching` of `SynchronizerConnectionConfig` in the admin API shows it.

### Synchronizer operators

#### Removal of support for sequencer drivers
- Sequencer drivers (such as CometBFT) are no longer supported.
- Reference sequencer is also no longer supported.

#### Named sequencer throughput cap settings

Sequencer operators should migrate throughput cap configuration to named settings and must update Admin API clients. Settings are now configured under
`canton.sequencers.<sequencer>.sequencer.block.throughput-cap.messages.<name>`.
Each setting must specify its `request-type` (for example, `"confirmation request"`) and can set
`observation-period-seconds` (default: 60), alongside its throughput limits.
An explicitly configured `messages` map replaces the default set of settings.
Multiple settings can apply to the same message type, and all must allow a submission.

To migrate, add `request-type` to every configured setting and move any global
`observation-period-seconds` or per-setting `observation-period-seconds-override` value to
`observation-period-seconds` within each affected setting.

Remove `delayed-activation`: caps are now enforced immediately, without an initial grace period.
Legacy fixed message-type configurations remain supported and log a deprecation warning.
Their caps inherit the parent `observation-period-seconds` (default: 60), and omitted legacy caps
retain their previous defaults. The new commitment and time-proof defaults are also included.
The legacy `delayed-activation` field is accepted but ignored.

The sequencer Admin API and console get/set/update/unset commands now identify settings by name
instead of message type. Update callers to pass the setting name and include the message type and
observation period in each setting value. `traffic_control.list_throughput_caps()` lists all settings.
Declarative cap configuration likewise uses setting names as keys and includes the message type and
observation period in each value.
Throughput-cap metric names now use the setting name rather than the message type, so update monitoring
queries when using different setting names.

When throughput caps are enabled, the default settings now also cap time proofs and commitments.
Operators configuring their own `messages` map must include this setting to apply the time-proof
and commitments cap.

### Removal of deprecated features

<!-- Features deprecated in an earlier release that this release removes: the feature, the release that deprecated
it, and its replacement. -->

### Removal of protocol versions

<!-- Protocol versions no longer supported: the versions, and how to upgrade synchronizers still on them. -->

## Deprecations

<!-- The deprecated item, its replacement, and when it will be removed. -->

- Deprecated the `canton.participants.<participant>.parameters.ledger-api-server.indexer.batching-parallelism` configuration setting, which is no longer supported.
- The traffic enforcement endpoints deprecated in 3.7 (`TrafficService.PruneEvents`, the JSON endpoint `POST /v2/traffic/events/prune` and the console command `ledger_api.traffic.prune_events`) are now disabled by default, and calls fail with `DEPRECATED_API_DISABLED`. Use `TrafficService.Prune` instead.
  Setting `canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-37 = true` re-enables them until their removal in 3.9.

## Minor Improvements
- The representative package id set by `repair.rollback_unassignment` will be the source validation package used in the unassign.

- The `Request-Timeout` header of JSON Ledger API requests now also accepts a time unit, for example `15s`. Values without a time unit are still interpreted as milliseconds.
- JSON Ledger API list (non-websocket) endpoints now enforce a maximum stream duration, cancelling the backing stream and returning `408 Request Timeout` instead of running unbounded. If a `Request-Timeout` header is not provided, the stream is bounded by the server's `canton.participants.<participant>.http-ledger-api.request-timeout` (default 20s); when the header is set, its value applies instead. A new `websocket-config` parameter `http-list-max-wait-time` (default 5s) caps the client-supplied idle wait.
- Indexer pipeline now breaks up heavy transaction to evenly sized batches, where the 'size' is determined by the number of database operations.
- The interactive submission endpoints `ExecuteSubmissionAndWait` and `ExecuteSubmissionAndWaitForTransaction` (gRPC and JSON Ledger API) now enforce `maximum-number-of-signatures-per-party`, rejecting requests with too many signatures per party, as `ExecuteSubmission` already did.
- Reads and writes from the active contract journal now use a batch aggregator for efficiency.
- The restart delay for a sequencer connection now only resets back to `min_restart_delay` if it stayed validated for at least the new `min_stable_duration` (default 10 seconds).
  This prevents a restarted connection that keeps failing shortly after validation from being restarted again too quickly.
- Gracefully handle an error on configuring multiple aliases for the same logical synchronizer with different physical synchronizer ids
- Upgrade to bouncy castle 1.87 to address security vulnerabilities in 1.86 (e.g. BDSA-2026-40175)
- The default sychronizer transaction parse tree depth limit has been set to 64. This applies to newly bootstrapped synchronizers on all supported Protocol Versions, including successor synchronizers of an LSU.

### Ledger API

- Ledger API `UpdateService`: participant authorization topology events (`TopologyFormat.include_participant_authorization_events`) are now publicly available.
  Previously `ReadAs` rights for every listed party (or `ReadAsAnyParty` for an empty party list) were required.
  The authorization required by the transaction and reassignment formats of the same request is unchanged.
- When reassigning if no validation package override is provided pick the highest commonly vetted package version.
- `PartyManagementService.ListKnownParties`: subsequent pages return the topology state seen when the first page was
  requested. Page tokens issued by another participant are rejected.
- JSON API endpoints now consistently return HTTP 401 (Unauthorized) rather than 403 (Forbidden) when a valid JWT token is not present.
- The repair service operations are now stored as dedicated event types in the indexer DB. This change has no impact neither on the repair service API nor the Ledger API update/ACS serving.

### Multi-Sync
- When contracts are automatically reassigned to allow command execution on a target synchronizer:
  - The package used for contract validation on the target synchronizer will be a package-id bound to the contract in the transaction so is guaranteed to be valid and vetted.
  - If a package bound to a contract in the transaction is vetted on the source synchronizer this will be used, otherwise the highest vetted version of the package will be used.

### All node operators

- The console commands `keys.secret.download` and `keys.secret.download_to` no longer require the preview feature flag.
  Downloading a key pair from a node still requires `canton.parameters.enable-key-download = true` on that node.
- The token generators shipped with Canton mint audience-based tokens: `ledger-api-auth generate-token` takes an
  `--audience` or, mutually exclusively, a `--participantId` whose participant audience is used, and the Ledger API bench tool
  takes `--user-based-authorization-audience` together with `--user-based-authorization-secret`.

### Participant node operators

- Previously `canton.remote-participants.<remoteParticipant>.token` was used for both ledger API and admin API commands in the Canton Console.
  Now, an optional `canton.remote-participants.<remoteParticipant>.admin-api-token` can be specified to use a different token for admin API
  commands in case that admin API auth differs from ledger API auth.
- The console now reads the user id for Ledger API commands from remote-participant tokens minted for a custom
  `target-audience` or `target-scope` as well. Previously such tokens made the console fall back to the
  `CantonConsole` user id.

### Synchronizer operators
- Initializing sequencers with the onboarding state of the current synchronizer or the LSU predecessor state now requires less JVM memory, because the
  initialization works in a streaming fashion instead of loading the entire topology state into memory first.
- When initializing a sequencer, the initial topology state is checked for consistency (e.g. no multiple transactions effective for the same unique mapping key at the same time).
  Some of the consistency checks potentially take a significant time to run for a large topology state (e.g. 10 million topology transactions),
  therefore they can be disabled with `topology.check-initial-topology-snapshot-consistency = false`.

- Sequencers reject submission requests and acknowledgements that carry more than one signature.
  Such requests that are ordered anyway are discarded as invalid ledger events from protocol version 38 on.
  On earlier protocol versions, only the last signature is kept after ordering, which is the one that was verified before.

## Security Fixes

### JDK Base Image Critical Fixes
- JDK base image has been upgraded to 21.0.13.1_2, incorporating several key security patches

<!--
Advisories first, then the other security fixes as a bullet list, including dependency updates for security
vulnerabilities (e.g. "Dependency updates to address security vulnerabilities: jackson 2.22.4, gRPC 1.85").
Remove this section if there are no security fixes.
-->

<!--
Advisory for a bug fix with a severity, e.g. (25-012, Low). For a Critical or High security fix, only once the
fix is deployed on MainNet:

### (YY-nnn, Risk): Title

#### Issue Description

#### Affected Deployments

#### Affected Versions

#### Impact

#### Symptom

#### Workaround

#### Likeliness

#### Recommendation
-->

## Bug Fixes

<!-- The other bug fixes as a bullet list, not split by audience. -->

- The JSON Ledger API HTTPS server now uses the `PKIX` trust manager and key manager instead of the legacy `SunX509` ones.
  `PKIX` performs full RFC 5280 certification path validation, so certificate chains that were previously accepted
  but violate constraints such as the name constraints of an intermediate CA are now rejected.
- The JSON Ledger API now bounds the size of each inbound websocket message by its `max-inbound-message-size`
  (default 10 MiB), as it already did for HTTP request bodies. A message above the limit, including one split into
  many fragments, is rejected with the new error code `JSON_API_WEBSOCKET_MESSAGE_TOO_LARGE` before it is assembled
  in memory, and the connection is closed. Binary websocket messages, which no JSON Ledger API endpoint accepts,
  are rejected with the new error code `JSON_API_WEBSOCKET_BINARY_MESSAGE_NOT_SUPPORTED` without being read.
- Minor bugfix to synchronization of `participants.parties.enable` which always timed out on a network
  with more than 100 parties.
- ACS commitments tolerate when a party changes its hosting state on a participant from "actively hosted" to back "onboarding".
- `enable-cert-revocation-checking` on a server TLS configuration (for example the Ledger API) now applies only to that server's TLS context.
  It no longer sets the JVM-wide `com.sun.net.ssl.checkRevocation`, `com.ibm.jsse2.checkRevocation` and `ocsp.enable` properties, so other TLS connections in the same node are no longer affected.
  Deployments that relied on this side effect to get revocation checking elsewhere must set these properties explicitly (for example with `-Dcom.sun.net.ssl.checkRevocation=true`).
  The flag is now also honored by the JSON Ledger API (`http-ledger-api`) and the BFT orderer's peer-to-peer server endpoint.
- Fixed a bug in `topology_util.sh` where `compute_canton_hash` intermittently produced a wrong hash, causing certificates from `prepare-cert.sh` to be rejected.
- Fixed the JSON Ledger API failing every HTTPS connection when the Ledger API TLS configuration has no
  `trust-collection-file`, which also affected plain server-side TLS with `client-auth` set to `none`.
  If client authentication is enabled (`client-auth` set to `optional`, the default, or `require`) and no
  `trust-collection-file` is configured, client certificates are now verified against the JVM default trust store,
  consistent with the gRPC Ledger API and the documentation.
  No configuration change is needed, and configurations with a `trust-collection-file` behave as before.
- Fixed a bug in the BFT sequencer where a topology change could go unnoticed by participants and mediators when its
  effective time coincided exactly with the end of an ordering epoch: the topology tick at that time was sent to
  the sequencers only, so other members did not observe the change becoming effective until other traffic or a time
  proof reached them.
- Fixed spurious `Locked connection was lost, trying to rebuild` warnings and unnecessary reconnects of DB locked connections
  on busy nodes: a read-only check that cannot run because the connection is in use no longer counts towards
  `replication.connection-pool.connection.max-inconclusive-read-only-checks`.
- `topology.transactions.generate_onboarding_transactions` now generates the transactions from the given `temporaryStore`.
  Previously, a participant returned the identity transactions of its authorized store instead whenever they had the
  requested protocol version, which ignored a delegation to an intermediate key provided in the temporary store when the
  root namespace key is offline.
- Added missing synchronization between ACS import and synchronizer disconnect/connect.
- Fixed a bug where a submission that explicitly requests a mediator group (as interactive submissions do, always
  requesting group `0`) interpreted the group index as a position in the list of active mediator groups. When group `0`
  was not active and only a higher-numbered group was, the submission was routed to the wrong group, and requesting that
  higher group failed with a `NoMediatorError`. The group is now looked up by its index, and the submission fails with
  a `NoMediatorError` if that group is not active.
- Early reassignment rejections now report the request's submission id in their completion instead of an empty one.
- Java bindings: `User.Right` now reads `CanActAsAnyParty`, which used to throw. `CommandsSubmission.toProto` now
  includes the prefetch contract keys, which it used to drop. `UpdateSubmission.withSynchronizerId` now sets the
  synchronizer id, which it used to ignore.

### CantonBFT

- Fixed race conditions identified in P2P connection teardown.

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
