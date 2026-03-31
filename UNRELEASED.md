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

### Sequencer Inspection Service
A new service is available on the Admin API of the sequencer.
It provides an RPC that allows to query for traffic summaries of sequenced events.
Refer to the [traffic documentation](https://docs.digitalasset.com/subnet/3.4/howtos/operate/traffic.html) for more details.

### Protocol Changes

#### Protocol Version 35

##### Multi-Synchronizer feature flag

- Multi-synchronizer is available in early access and has to be enabled explicitly.
  This feature should only be used in test environments.

- To enable contract reassignment across synchronizers, this flag must be activated on all participants
  hosting a stakeholder of the contract on both the source and target synchronizers. For a synchronizer,
  it can be done as follows:

```
participant.topology.synchronizer_trust_certificates.propose(
  p.id,
  synchronizerId,
  featureFlags = Seq(ParticipantTopologyFeatureFlag.EnableUnsafeMultiSynchronizer),
)
```

##### Time to Live (TTL) and Hashing Algorithm Update

- A new hashing scheme version `HASHING_SCHEME_VERSION_V3` has been introduced that includes the TTL in the hash computation.
- See the [hashing algorithm documentation](https://docs.digitalasset-staging.com/build/3.5/explanations/external-signing/external_signing_hashing_algorithm) for the updated version.
- The `TTL` is now enforced at confirmation time by all confirming participants.

##### SynchronizerId field update in Externally signed transactions
- In Protocol version 35, the `synchronizer_id` field in externally signed prepared transaction metadata
  will be populated with the physical synchronizer ID of the synchronizer on which the transaction will be processed,
  instead of the logical synchronizer ID, as is the case in PV 34.
  Applications must ensure they do not rely on the format of the `synchronizer_id` value.

#### Dev Protocol


### Minor Improvements
- *BREAKING* We reduced the defaults for `setBalanceRequestSubmissionWindowSize` and `defaultMaxSequencingTimeOffset`
  to 2 minutes.
- The Ledger JSON API `v2/package-vetting` endpoint exposes list functionality on the GET method by accepting a request body. This is not recommended by the HTTP specification, hence the endpoint is deprecated.
  For consistency, the POST method, used for updating the vetting state, of the same endpoint is also deprecated.
  In turn, two new endpoints are implemented to provide the same functionality:
  - `v2/package-vetting/list` accepts a POST request with the same body as the deprecated GET `v2/package-vetting` endpoint and returns the list of vetted packages in the same format.
  - `v2/package-vetting/update` accepts a POST request with the same body as the deprecated POST endpoint `v2/package-vetting` and returns the updated vetting state of the package in the same format.
- JSON Ledger API OpenAPI/AsyncAPI spec corrections
  - Fields not marked as required in the Ledger API `.proto` specification are now also optional in the OpenAPI/AsyncAPI specifications.
    If your client code is using code generated using previous versions of these specifications, it may not compile or function correctly with the new version. To migrate:
    - If you prefer not to update your code, continue using the previous specification versions as the JSON API server preserves backward compatibility.
    - If you want to use new endpoints, features or leverage the new less strict spec, migrate to the new OpenAPI/AsyncAPI specifications as follows:
      - Java clients: No changes are needed if you use the `OpenAPI Generator`. Otherwise, potentially optionality of fields should be handled appropriately for other code generators.
      - TypeScript clients: Update your code to handle optional fields, using the `!` or `??` operators as appropriate.
  - From Canton 3.5 onwards, OpenAPI/AsyncAPI specification files are suffixed with the Canton version (e.g., `openapi-3.5.0.yaml`).
  - Canton 3.5 is compatible with OpenAPI specification files from version 3.4.0 to 3.5.0 (inclusive).
- Added support for adding table settings for PostgreSQL. One can use a repeatable migration (Flyway feature) in a file
  provided to Canton externally.
  - Use the new config `repeatable-migrations-paths` under the `canton.<node_type>.<node>.storage.parameters` configuration section.
  - The config takes a list of directories where repeatable migration files must be placed, paths must be prefixed with `filesystem:` for Flyway to recognize them.
  - Example: `canton.sequencers.sequencer1.storage.parameters.repeatable-migrations-paths = ["filesystem:community/common/src/test/resources/test_table_settings"]`.
  - Only repeatable migrations are allowed in these directories: files with names starting with `R__` and ending with `.sql`.
  - The files cannot be removed once added, but they can be modified (unlike the `V__` versioned schema migrations), and if modified these will be reapplied on each Canton startup.
  - The files are applied in lexicographical order.
  - Example use case: adding `autovacuum_*` settings to existing tables.
  - Only add idempotent changes in repeatable migrations.
- Changed the path for `crypto.kms.session-signing-keys` (deprecated) to `crypto.session-signing-keys` so that session signing key configuration is no longer directly tied to a KMS. However, session signing keys can still only be enabled when using a KMS provider or when running with `non-standard-config=true`.
- Added a new configuration parameter for session signing keys, `toleranceShiftDuration`, and updated `cutOffDuration` to allow a zero duration.
- Offline root namespace key scripts:
  - Renamed `prepare-certs.sh` to `prepare-cert.sh`
  - Changed `assemble-certs.sh` to automatically suffix the generated certificate with a `.cert` extension, similarly to what is being done in `prepare-cert.sh`
  - Removed the `10-offline-root-namespace-init` example folder as its content is now integrated in the documented how-to: https://docs.digitalasset.com/operate/3.5/howtos/secure/keys/namespace_key.html
  - Committed the buf image necessary to run the script to the repository (also available in the release artifact), making usage from the open source repo easier
- Ledger JSON Api changes:
    - The Ledger JSON API server now enforces that only fields marked as required by the Ledger API OpenAPI/AsyncAPI specification are mandatory in request payloads.
- ApiRequestLogger now also used by Ledger JSON Api. Changes:
    - Redundant Request TID removed from logs.
    - Additional CLI options added: `--log-access`, `--log-access-errors`...
    - Additional config options added: `debugInProcessRequests`, `prefixGrpcAddresses`
- `package-dependency-cache` field in `caching` configuration is deprecated. It can be removed safely from node configurations.
- The `generateExternalPartyTopology` endpoint on the Ledger API now returns a single `PartyToParticipant` topology transaction to onboard the party.
The transaction contains signing threshold and signing keys. This effectively deprecate the usage of `PartyToKeyMapping`.
For parties with signing keys both in `PartyToParticipant` and `PartyToKeyMapping`, the keys from `PartyToParticipant` take precedence.
- Batching configuration now allows setting different parallelism for pruning (currently only for Sequencer pruning):
  New option `canton.sequencers.sequencer.parameters.batching.pruning-parallelism` (defaults to `2`) can be used
  separately from the general `canton.sequencers.sequencer.parameters.batching.parallelism` setting.
- Made the config option `...topology.use-time-proofs-to-observe-effective-time` work and changed the default to `false`.
  Disabling this option activates a more robust time advancement broadcast mechanism on the sequencers,
  which however still does not tolerate crashes or big gaps in block sequencing times. The parameters can be configured
  in the sequencer via `canton.sequencers.<sequencer>.parameters.time-advancing-topology`.
- LedgerAPI ListKnownParties supports an optional prefix filter argument filterParty.
  The respective JSON API endpoint now additionally supports `identity-provider-id` as
  an optional argument, as well as `filter-party`.
- Protect the admin participant from self lock-out. It is now impossible for an admin to remove own admin rights or
  delete itself.
- *BREAKING* The default OTLP gRPC port that the Canton connects to in order to export the traces has been changed from
  4318 to 4317. This aligns the default configuration of Canton with the default configuration of the OpenTelemetry
  Collector. This change affects only the users who have configured an OTLP trace export through
  ```
  canton.monitoring.tracing.tracer.exporter.type=otlp
  ```
- On Ledger API interface subscriptions, the `CreatedEvent.interface_views` now returns the ID of the package containing
  the interface implementation that was used to compute the specific interface view as `InterfaceView.implementation_package_id`.
- The Postgres connection tuning configuration of the indexer is now separated from the configuration of the Ledger API server
  (`canton.participants.<participant>.ledger-api.postgres-data-source`).
  The new configuration section `canton.participants.<participant>.parameters.ledger-api-server.indexer.postgres-data-source` should
  be used instead to tune the indexer's Postgres connections.
- Added network timeout and client_connection_check_interval for db operations in the Ledger API server and indexer to avoid
  hanging connections for Postgres (see PostgresDataSourceConfig). The defaults are 60 seconds network timeout and
  5 seconds client_connection_check_interval for the Ledger API server, and 20 seconds network timeout and
  5 seconds client_connection_check_interval for the indexer. These values can be configured via the new configuration parameters
  `canton.participants.<participant>.ledger-api.postgres-data-source.network-timeout` for network timeout of the Ledger API
  server and `canton.participants.<participant>.parameters.ledger-api-server.indexer.postgres-data-source.client-connection-check-interval`
  for the client_connection_check_interval of the indexer.
- We have changed the way that OffsetCheckpoints are populated to always generate at least one when an open-ended
  updates or completions stream is requested. An OffsetCheckpoint can have offset equal to the exclusive start for which
  the stream is requested. This ensures that checkpoints are visible even when there are no updates, and the stream was
  requested to begin exclusively from the ledger end.
- Extended the set of characters allowed in user-id in the ledger api to contain brackets: `()`.
  This also makes those characters accepted as part of the `sub` claims in JWT tokens.
- A new indexer pipeline batching strategy added under the feature flag `useWeighetdBatching`. When switched on, the
  batches are created using their estimated database processing time using the `submissionBatchInsertionSize` as a limit
  for individual batches
- Additional metrics for the ACS commitment processor: `daml.participant.sync.commitments.last-incoming-received`, `daml.participant.sync.commitments.last-incoming-processed`, `daml.participant.sync.commitments.last-locally-completed`, and `daml.participant.sync.commitments.last-locally-checkpointed`.
- *BREAKING* Removed the `LastErrorsAppender` along with the Admin API endpoints `StatusService.GetLastErrors` and `StatusServiceGetLastErrorTrace`, as
  well as the corresponding console commands `last_errors` and `last_error_trace`.
- Changed the `CompressedBatch` structure in the sequencer protocol for protocol version 35 to separately keep recipients and envelopes (from `gzip(Seq((recp1, payload1), (recp2, payload2)))` to `gzip(Seq(recp1, recp2)), Seq(gzip(payload1), gzip(payload2)))`).
- The configuration parameters `topology.use-new-processor` and `topology.use-new-client` have been deprecated and now default to true. Configuring those parameters to false will be ignored.
- Functionality for managing internal and external parties has been improved, removing previous asymmetry:
  - User rights can now be assigned to an external party during allocation.
  - External parties can be allocated by the user themselves in the self-administration mode.
  Please note that users in self-administration mode can allocate up to N parties, depending on a setting of the parameter
  ```
  canton.participants.<participant-id>.ledger-api.party-management-service.max-self-allocated-parties
  ```
  By default the value of this parameter is 0.
- An IDP administrator can now only allocate parties confined to their own IDP perimeter.
- The Ledger API and Ledger JSON API prepare `InteractiveSubmissionService` has been modified to support a such that
- `PrepareSubmissionRequest.hashing_scheme_version` can now be populated with the desired hashing version to be
   used for the transaction. The default hashing scheme is `HASHING_SCHEME_VERSION_V2` but integrators are encouraged to move to `HASHING_SCHEME_VERSION_V3` for
  synchronizers using protocol version 35.
- Topology-Aware Package Selection (TAPS) refinement for handling inconsistent vetting states:
  - The algorithm now considers a party's package vetting state only for packages required by that party in the interpreted transaction.
    It starts with a minimal set of restrictions derived from the command's root nodes and progressively accumulates more restrictions over a configurable number of passes.
    This iterative process increases the likelihood of finding a valid package selection set for the routing of the transaction.
  - The maximum number of TAPS passes can be set at the request-level via the optional `taps_max_passes` field in `Commands` or `PrepareSubmissionRequest` messages.
    If not specified, the default value is taken from the participant configuration via `participants.participant.ledger-api.topology-aware-package-selection.max-passes-default` (defaults to `3`).
    A hard limit is enforced by `participants.participant.ledger-api.topology-aware-package-selection.max-passes-limit` (defaults to `4`).
- Deprecated: removed the feature flag `canton.sequencers.<node>.parameters.async-writer.enabled`, as async writing is now
  the only supported mode.
- Added a field `MaxConcurrentStreamsPerConnection` and corresponding default
`defaultMaxConcurrentStreamsPerConnection` (set to 500) to `ServerConfig`.
This corresponds to `max-concurrent-streams-per-connection` in the app configs, e.g.,
`docker/canton/images/canton-sequencer/app.conf` and can be changed there. At present
the value for sequencers is  configured to be 500 for the public API and 100 for the Admin API.
- Deprecated: the parameter
  `canton.participants.<participant>.parameters.package-metadata-view.init-takes-too-long-interval`
  is now ignored, and a warning will only be printed once, rather than periodically.
- Deprecated: the parameter
  `canton.participants.<participant>.parameters.ledger-api-server.indexer.prepare-package-metadata-time-out-warning`
  is now ignored.
- Deprecated usage of `PartyToKeyMapping`. The functionality provided by `PartyToKeyMapping` is now available directly in `PartyToParticipant`.
  Please use `PartyToParticipant` for new transactions. `PartyToKeyMapping` is still fully supported in this version (including existing and new transactions).
  In future version, creation of new `PartyToKeyMapping` transactions may be disallowed.

### Enhanced Reliability for `GetHighestOffsetByTimestamp`

Previously, the `GetHighestOffsetByTimestamp` RPC and the `find_highest_offset_by_timestamp` console command could return offsets not yet synced with the participant's local cache. Furthermore, forcing a query with a future timestamp resulted in an error.

Specific changes:
- The required state is now retrieved atomically via a consistent database snapshot.
- The endpoint now includes an internal barrier (waiting up to 10 seconds) to ensure the local Ledger API cache catches up with the database before returning the offset.
- When `force` is true, requesting a future timestamp now gracefully returns the current ledger end instead of failing.

No migration required.

### Preview Features
- preview feature

### Bugfixes
- Switched the gRPC service `SequencerService.subscribe` and `SequencerService.downloadTopologyStateForInit` to manual
  control flow, so that the sequencer doesn't crash with an `OutOfMemoryError` when responding to slow clients.

- Fixed a bug preventing automatic synchronization of protocol feature flags.
  Automatic synchronization can be disabled by setting `parameters.auto-sync-protocol-feature-flags = false` in the participant's configuration object.

- Fixed a bug where the Ledger API `PackageService.ListVettedPackages` used to return a potentially not yet
  effective state of the vetted packages. Now it returns the state of vetted packages effective at the time of the request.

- Sequencer health status used to incorrectly return the synchronizer uid instead of the sequencer uid.

### (YY-nnn, Severity): Title

#### Issue Description

#### Affected Deployments

#### Affected Versions

#### Impact

#### Symptom

#### Workaround

#### Likeliness

#### Recommendation


## Other changes

### Removal of the old sequencer connection transports
The old sequencer connections tranports have been removed, and only the new sequencer connection pool remains.
Consequently, the configuration `<node>.sequencer-client.use-new-connection-pool` has been deprecated and no longer has any effect.

### Changes from NonNegativeLong to Long
Some console commands using a NonNegativeLong for the offset are changed to accept a Long instead.
Similarly, some console commands returning an offset now return a Long instead of a NonNegativeLong.
It brings consistency and allows to pass the output of `participant.ledger_api.state.end()`.

Impacted commands:
- `participant.repair.export_acs`
- `participant.parties.find_party_max_activation_offset`
- `participant.parties.find_party_max_deactivation_offset`
- `participant.parties.find_highest_offset_by_timestamp`

### Removal of automatic recomputation of contract ids upon ACS import
The ability to recompute contract ids upon ACS import has been removed.

### Online party replication

- Added the file-based online party replication command `participant.parties.add_party_with_acs_async` to
be used along with `participant.parties.export_party_acs` and instead of the sequencer-channel-based
`add_party_async` command.
- The online party replication status command now returns status in a very different, "vector-status" format
rather than the old "oneof" style. This impacts the `participant.parties.get_add_party_status` command and
`com.digitalasset.canton.admin.participant.v30.PartyManagementService.GetAddPartyStatus` gRPC response type.
- The participant configuration to enable online party replication has been renamed to
`alpha-online-party-replication-support` from `unsafe-online-party-replication` for consistency with other
alpha features and to reflect that the default file-based mode is more secure not relying on sequencer
channels.
- The sequencer configuration to enable sequencer channels for online party replication has been renamed to
`unsafe-sequencer-channel-support` from `unsafe-enable-online-party-replication` for consistency and to
refer specifically to sequencer channels.

### Offline party replication

Concluding an offline party replication by clearing the onboarding flag now includes two major updates:
- Added crash resilience for ongoing clearances.
- Automatic scheduling for clearances when a participant (re)connects to the synchronizer.

These changes apply only to the `participant.parties.import_party_acs` and
`participant.parties.clear_party_onboarding_flag` endpoints.

Note: The replicated party ID must be included in the party ACS import call to enable automatic
scheduling.

### Alpha Multi-Synchronizer Support

Adds a new participant node parameter, `alpha-multi-synchronizer-support` (Boolean).
- **Default (`false`):** Uses standard **Create** and **Archive** events.
- **Enabled (`true`):** Uses **Assign** and **Unassign** events.

This flag is required in multi-synchronizer environments to preserve the **reassignment counter** of a contract.
Using the default (Create events) resets this counter to zero.

Note: Multi-synchronizer support is currently in Alpha; most Ledger API consumers may not yet be compatible with
Assign/Unassign events. Only enable this if your application specifically requires non-zero reassignment counters
and can process these event types.

### Removal of legacy party replication repair console macros

The original party replication method, which relied on a silent synchronizer, has been superseded by the offline party
replication process. Consequently, the obsolete repair console macros associated with the legacy approach have
been removed.

Specifically, the following macros are no longer available:
- `step1_hold_and_store_acs`
- `step2_import_acs`

If you previously relied on the _Silent synchronizer replication procedure_, you will need to transition to the
current offline party replication process. For details, please consult the
[Offline Party Replication documentation](https://docs.digitalasset.com/operate/3.5/howtos/operate/parties/party_replication.html#offline-party-replication)

### Only PackageName is accepted on Ledger API

- *BREAKING* Usage of package id for ledger queries was deprecated and now the validation will fail if used.
  The impacted APIs are:
    - GetUpdates
    - GetUpdateByOffset
    - GetUpdateById
    - GetActiveContracts
    - GetEventsByContractIdRequest
    - SubmitAndWaitForTransaction (the optional `transaction_format`)
    - SubmitAndWaitForReassignmentRequest
    - ExecuteSubmissionAndWaitForTransactionRequest

### Party replication onboarding topology event is exposed on Ledger API

The `PartyToParticipant` topology "onboarding" state used in the process of replicating a party with existing
contracts is now visible via the Ledger API when a party onboards on a synchronizer on protocol version 35 or higher.
Starting with PV=35, the newly introduced `ParticipantAuthorizationOnboarding` Ledger API topology event signals
the beginning of party replication and transitions to `ParticipantAuthorizationAdded` once the party's ACS is fully
visible on the Ledger API.

### ACS stream continuation

The `GetActiveContracts` stream request has been extended with an optional `stream_continuation_token` field that allows
clients to continue an interrupted ACS stream from the last element which made through. The field can be populated with
the `stream_continuation_token` field of the last response element received before the interruption, and the stream will
continue from the next element after that.

### GetUpdates stream in descending order of events

The `GetUpdatesRequest` object has new optional parameter `descending_order`. When this parameter is `true` the events
are streamed from the newest to the oldest ones.

### Removal of deprecated, legacy ACS export and import endpoints

The legacy repair endpoints for the ACS export and import have been removed:

- Console command `participant.repair.export_acs_old`
- Console command `participant.repair.import_acs_old`
- gRPC rpc `ParticipantRepairService.ExportAcsOld`
- gRPC rpc `ParticipantRepairService.ImportAcsOld`

#### Migration advice

Use repair endpoints without the 'old' suffix:

- Migrate to `participant.repair.export_acs` from `participant.repair.export_acs_old`
- Migrate to `participant.repair.import_acs` from `participant.repair.import_acs_old`
- Migrate to `ParticipantRepairService.ExportAcs` from `ParticipantRepairService.ExportAcsOld`
- Migrate to `ParticipantRepairService.ImportAcs` from `ParticipantRepairService.ImportAcsOld`

Note that previously created ACS snapshots with the legacy endpoints cannot be imported with the current endpoints as
the underlying data format has completely changed.

##### Migrating to export_acs

The most significant change is the removal of the `timestamp` parameter, which has been replaced by a mandatory
`ledgerOffset` parameter.

**Console parameter changes:**

- **New mandatory parameter:** `ledgerOffset (Long)`. You must now specify the exact ledger offset for the snapshot
  instead of a `timestamp`.
- **Removed parameters:** `partiesOffboarding`, `timestamp` (replaced by `ledgerOffset`), `force`.
- **Renamed parameters:** `outputFile` is now `exportFilePath` (default is `"canton-acs-export.gz"`),
  `filterSynchronizerId` is now `synchronizerId`.
- **New optional parameters:** `excludedStakeholders` allows you to omit contracts that have one or more of these
  parties as a stakeholder; `contractSynchronizerRenames` allows mapping contracts from one synchronizer to another
  during export.

**gRPC changes for `ExportAcsRequest`:**

- **`parties` -> `party_ids`:** Field renamed for consistency. If left empty, the endpoint will act as a wildcard and
  export the ACS for *all* parties hosted by the participant.
- **`timestamp` -> `ledger_offset` (Breaking):** You must provide an exact `int64 ledger_offset` instead of a timestamp.
- **`filter_synchronizer_id` -> `synchronizer_id`:** Field renamed for consistency.
- **Removed fields:** `force` and `parties_offboarding` have been completely removed.
- **New fields:** `contract_synchronizer_renames` and `excluded_stakeholder_ids`.

##### Migrating to import_acs

The import command remains largely the same in basic usage, but introduces new optional parameters for advanced
validation and overrides, alongside strict memory-efficient streaming semantics for gRPC.

**Console parameter changes:**

- **Renamed parameter:** `inputFile` is now `importFilePath` (default is `"canton-acs-export.gz"`).
- **New optional parameters:** `contractImportMode` governs contract validation upon import (defaults to
  `ContractImportMode.Validation`); `representativePackageIdOverride` allows overriding representative package IDs
  during import; `excludedStakeholders` allows omitting contracts that have one or more of these parties as a
  stakeholder.

**gRPC changes for `ImportAcsRequest`:**

- **Streaming Semantics (Breaking):** The new endpoint requires metadata fields (like `contract_import_mode`,
  `synchronizer_id`, etc.) to be populated *only* in the first request of the stream. Subsequent requests must omit
  metadata and only contain the binary `acs_snapshot` chunks.
- **New mandatory fields:** `contract_import_mode` and `synchronizer_id` must be explicitly defined in the first stream
  request.
- **Removed fields:** `allow_contract_id_suffix_recomputation` is completely removed.
- **New fields:** `excluded_stakeholder_ids` and `representative_package_id_override`.
- **Response update:** `ImportAcsResponse` is now a completely empty message (previously returned a contract ID
  mapping).

### Improvements for `repair.add` and migration advice

The `participant.repair.add` admin command has been revised to use the new `ImportAcs` backend, bringing significant
memory performance improvements, stricter default safety validations, and several new parameters.

#### Important behavioral change: strict `Validation` by default

Previously, `repair.add` implicitly accepted all injected contracts without re-evaluating their cryptographic hashes. To
prevent accidental data corruption, the command now defaults to **Validation** mode (
`contractImportMode = ContractImportMode.Validation`).

- **Impact:** If you have existing scripts or recovery procedures that inject manually modified, synthetic, or
  inconsistent contracts (where the payload does not strictly match the `ContractId` hash), they will now fail with a
  `"Failed to authenticate contract with id"` error.
- **Migration:** To bypass this cryptographic validation and restore the legacy behavior, explicitly pass the `Accept`
  mode in your command call:
    ```scala
    participant.repair.add(
      synchronizerId = mySynchronizer,
      protocolVersion = myProtocolVersion,
      contracts = myContracts,
      contractImportMode = ContractImportMode.Accept // Bypasses strict validation
    )
    ```

#### New parameters

The command signature has been expanded to support several optional parameters:

- `workflowIdPrefix`: Allows you to set a custom prefix for the generated workflow ID to easily track the repair
  transactions (defaults to `import-<UUID>`).
- `contractImportMode`: Choose between `Validation` (default, validates that contract IDs comply with the scheme
  associated to the synchronizer where the contracts are assigned), or `Accept` the contracts as they are (if you know
  what you are doing).
- `representativePackageIdOverride`: Allows you to remap or override the representative package IDs of the contracts as
  they are imported.
- `excludedStakeholders`: When defined, any contract that has one or more of these parties as a stakeholder will not be
  added.

### Improved party and repair ACS imports

We have completely overhauled the ACS import endpoints for both party replication and participant repair to be
memory-efficient streaming endpoints:

- Console command `participant.parties.import_party_acs`
- Console command `participant.repair.import_acs`
- gRPC RPC `PartyManagementService.ImportPartyAcs`
- gRPC RPC `ParticipantRepairService.ImportAcs`

This resolves previous memory limitations, as these endpoints no longer load the entire ACS snapshot into memory at
once.

#### Action required: Breaking API change

The `synchronizerId` is now a **mandatory** first parameter for both the `import_party_acs` and `import_acs` console
commands as well as their analogous gRPC endpoints. You will need to update any existing scripts.

**For `import_party_acs`:**

- **Old usage:** `participant.parties.import_party_acs("canton-acs-export.gz")`
- **New usage:** `participant.parties.import_party_acs(mySynchronizerId, importFilePath = "canton-acs-export.gz")`

**For `import_acs`:**

- **Old usage:** `participant.repair.import_acs("canton-acs-export.gz")`
- **New usage:** `participant.repair.import_acs(mySynchronizerId, importFilePath = "canton-acs-export.gz")`

Because of the mandatory `synchronizerId` parameter, to import a multi-synchronizer ACS snapshot, you must now call the
endpoint sequentially for each synchronizer your participant is connected to, using the exact same snapshot file. The
import process will ignore any contracts in the snapshot that are associated to a different synchronizer.

##### Details on the gRPC `ImportAcs` repair endpoint

The `ImportAcs` and `ImportAcsV2` RPCs have been consolidated, introducing the following breaking changes and migration
steps:

- **Endpoint removed:** `ImportAcsV2` (along with its request/response messages) is completely removed. All clients must
  migrate to the standard `ImportAcs` RPC.
- **Request signature and type changes:**
    - Fields `workflow_id_prefix` (2), `contract_import_mode` (3), and `representative_package_id_override` (5) in
      `ImportAcsRequest` are now explicitly `optional`.
    - A new `optional string synchronizer_id = 6` field was added.
    - **Migration (ScalaPB):** Adding `optional` changes generated code from base types to `Option[T]`. Existing clients
      will fail to compile and must be updated to wrap assigned values (e.g., `workflowIdPrefix = Some("prefix")`) and
      explicitly handle reading `Option` types.
- **Behavioral change (`synchronizer_id`):** When filtering by synchronizer, mismatched contracts are now ignored. This
  breaks previous logic that relied on the import strictly aborting upon a mismatch.

##### Details on the gRPC `ImportPartyAcs` party replication endpoint

The `ImportPartyAcs` endpoint underwent the exact same consolidation (removing `ImportPartyAcsV2`), streaming semantics
updates, generated code changes (ScalaPB `Option[T]`), and mismatched synchronizer behavior (ignoring rather than
failing) as `ImportAcs`.

**Key differences specific to `ImportPartyAcs`:**

- **New capability (`party_id`):** A new `optional string party_id = 6` field was added. Providing this in the first
  request of the stream enables automatic, crash-resilient scheduling of the onboarding flag clearance. If omitted, the
  participant logs a warning, and the flag must be cleared manually.
- **Logical/Physical ID support:** The `synchronizer_id` (field 2) temporarily accepts either a logical or physical
  synchronizer ID to better support Logical Synchronizer Upgrade (LSU) scenarios. This support is subject to change.

### update to GRPC 1.77.0

removes [CVE-2025-58057](https://github.com/advisories/GHSA-3p8m-j85q-pgmj) from security reports.

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



