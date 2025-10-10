# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/vCANTON_VERSION). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-CANTON_VERSION.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/CANTON_VERSION/canton/about.html).

INFO: Note that the **"## Until YYYY-MM-DD (Exclusive)" headers**
below should all be Wednesdays to align with the weekly release
schedule, i.e. if you add an entry effective at or after the first
header, prepend the new date header that corresponds to the
Wednesday after your change.

## until 2025-10-15 (Exclusive)
- Add a `max_record_time` field in the `PrepareSubmissionRequest` for externally signed transaction.
  This is effectively a Time To Live (TTL) for the submission: it won't be committed to the ledger after that timestamp.
  Important: The enforcement of this TTL is done by the submitting node (NOT the confirming node of the external party).
  It is therefore unsigned and does not require a change to the transaction hashing scheme.
  This also means that the submitting node must be trusted to honor this field if used.
- new participant config parameter `canton.participants.<participant>.parameters.do-not-await-on-checking-incoming-commitments` to disable the synchronization of checking incoming commitments with crash recovery.
- Removed `contractSynchronizerRenames` on ACS import legacy.
- **BREAKING**: Removed force flag `FORCE_FLAG_ALLOW_UNVET_PACKAGE` and topology manager error `TOPOLOGY_DANGEROUS_VETTING_COMMAND_REQUIRES_FORCE_FLAG`.
  Unvetting a package is not a dangerous operation anymore.
- Added new endpoints to allocate external parties via the Ledger API in the `PartyManagementService`:
  - `GenerateExternalPartyTopology`: Generates topology transactions required to onboard an external party along with the pre-computed hash ready to sign
  - `AllocateExternalParty`: Allocates an external party on a synchronizer from a set of signed topology transactions
    Refer to the external party onboarding documentation for more details.
- **BREAKING**: The new party allocation endpoints above come with a breaking change for users of the gRPC `InteractiveSubmissionService` used in external signing workflows.
  - The following protobuf messages have been refactored to a new common protobuf package so they can be re-used across different services:

| Old file                                                                | Old FQN                                                 | New file                            | New FQN                                     |
|-------------------------------------------------------------------------|---------------------------------------------------------|-------------------------------------|---------------------------------------------|
| com/daml/ledger/api/v2/interactive/interactive_submission_service.proto | com.daml.ledger.api.v2.interactive.Signature            | com/daml/ledger/api/v2/crypto.proto | com.daml.ledger.api.v2.Signature            |
| com/daml/ledger/api/v2/interactive/interactive_submission_service.proto | com.daml.ledger.api.v2.interactive.SigningAlgorithmSpec | com/daml/ledger/api/v2/crypto.proto | com.daml.ledger.api.v2.SigningAlgorithmSpec |
| com/daml/ledger/api/v2/interactive/interactive_submission_service.proto | com.daml.ledger.api.v2.interactive.SignatureFormat      | com/daml/ledger/api/v2/crypto.proto | com.daml.ledger.api.v2.SignatureFormat      |

To consume the update, re-generate any client code generated from the protobuf definitions and update the package names accordingly in your application code.

## until 2025-10-08 (Exclusive)
- Fixed a bug in the ModelConformanceChecker that would incorrectly reject otherwise valid externally signed transactions
  that make use of a locally created contract in a subview.
- Added support for vetting packages on specific synchonizers, by adding an
  optional synchronizer_id field to the following messages:
    - `ValidateDarRequest`, `UploadDarRequest`, `VetDarRequest`, and
      `UnvetDarRequest` in the Admin API
    - `UploadDarFileRequest`, `ValidateDarFileRequest`, and
      `UpdateVettedPackagesRequest` in the Ledger API
    - These requests no longer use AuthorizedStore for vetting changes, they
      must specify a target synchronizer via the new synchronizer_id fields if
      the target participant is connected to more than one synchronizer.
- The `ExportPartyAcs` endpoint (`party_management_service.proto`), intended for offline party replication only, now
  requires the onboarding flag to be set in the party-to-participant topology transaction that activates the party on a
  target participant. The ACS export fails if this flag is missing.
- Setting DynamicSynchronizerParameters.participantResponseTimeout and mediatorReactionTimeout outside
  of the interval [1s, 5min] requires force flag.
- BREAKING: Moved the sequencer-api limits into the server config. Now, the same functionality is
  also available on the Admin API. The configuration is now under `<api-config>.stream.limits = { ... }`, for both the public and admin api.
- Added extra configuration for compressing data in indexer DB.


## until 2025-10-01 (Exclusive)
- Some methods in Ledger JSON API are now marked as deprecated in `openapi.yml`.
- Config `canton.sequencers.<sequencer>.sequencer.block.reader.use-recipients-table-for-reads = true` has been removed,
  `sequencer_event_recipients` table is now always used for reading from the sequencer via subscriptions.
- Config `canton.sequencers.<sequencer>.sequencer.block.writer.buffer-events-with-payloads = false // default` has been removed,
  events are always buffered without payloads now (payloads are cached separately).
- Config `canton.sequencers.<sequencer>.sequencer.block.writer.buffer-payloads = true // default` has been removed,
  payloads are always handed off to the cache now.
- Dropped redundant column `mediator_id` from `mediator_deduplication_store` table.
- Mediator deduplication store pruning is now much less aggressive:
    - running at most 1 query at a time
    - running at most once every configurable interval:
```hocon
  canton.mediators.<mediator>.deduplication-store.prune-at-most-every = 10s // default value
```
- Mediator deduplication now has batch aggregator configuration exposed for `persist` operation,
  allowing to tune parallelism and batch size under:
```hocon
  canton.mediators.<mediator>.deduplication-store.persist-batching = {
    maximum-in-flight = 2 // default value
    maximum-batch-size = 500 // default value
  }
```
- Added endpoints related to topology snapshots that are less memory intensive for the nodes exporting the topology snapshots:
    - `TopologyManagerReadService.ExportTopologySnapshotV2`: generic topology snapshot export
    - `TopologyManagerReadService.GenesisStateV2`: export genesis state for major upgrade
    - `TopologyManagerWriteService.ImportTopologySnapshotV2`: generic topology snapshot export
    - `SequencerAdministrationService.OnboardingStateV2`: export sequencer snapshot for onboarding a new sequencer
    - `SequencerInitializationService.InitializeSequencerFromGenesisStateV2`: initialize sequencer for a new synchronizer
    - `SequencerInitializationService.InitializeSequencerFromOnboardingStateV2`: initialize sequencer from an onboarding snapshot created by `SequencerAdministrationService.OnboardingStateV2`

- Added indices that speeds up various topology related queries as well as the update of the `valid_until` column.
- Add flag to disable the initial topology snapshot validation
  ```
  participants.participant1.topology.validate-initial-topology-snapshot = true // default value
  mediators.mediator1.topology.validate-initial-topology-snapshot = true // default value
  sequencers.sequencer1.topology.validate-initial-topology-snapshot = true // default value
  ```

- Add new endpoint to complete the party onboarding process for offline party replication by removing the onboarding flag.
  It's intended for polling (called repeatedly by the client), and removes the flag when the conditions are right.
  See `party_management_service.proto`; `CompletePartyOnboarding` rpc for more details.

- JSON Ledger API is now configured by default. It listens on the port 4003. It can be disabled by setting
  ```
  canton.participants.<participant>.http-ledger-api.enabled=false
  ```
- Introduced the ability for the user to self-administer. A user can now
    - query the details (`getParties`) of any the party it has any right to operate (ReadAs, ActAs, ExecuteAs or wildcard forms thereof)
    - query own user record (`getUser`)
    - query own rights (`listUserRights`)
    - allocate own party (`allocateParty`)

  There is a per-participant setting that defines maximum number of access rights that a user can have and still
  be able to self-allocate a party:
  ```
  canton.participants.<participant-id>.ledger-api.party-management-service.max-self-allocated-parties = 4
  ```
  The default is 0.
- `DisclosedContract.template_id` and `DisclosedContract.contract_id` for Ledger API commands
  are not required anymore. When provided, the fields are used for validation of the analogous fields in the encoded
  created event blob.
- Unvetting a package that is used as a dependency by another vetted package now requires `FORCE_FLAG_ALLOW_UNVETTED_DEPENDENCIES`
- The legacy gRPC Ledger API message `TransactionFilter` has been removed. As a consequence, the `filter` and `verbose` fields
  have been dropped from the `GetUpdatesRequest` and `GetActiveContractsRequest` messages.
- The JSON versions of the above gRPC messages **continue to be supported in their old form 3.4**.
  They will be removed and altered respectively 3.5. This affects:
  - `TransactionFilter` will be removed in 3.5
  - `GetUpdatesRequest` will be altered in 3.5, it is used in
    - `/v2/updates` HTTP POST and websocket GET endpoints
    - `/v2/updates/flats` HTTP POST and websocket GET endpoints
    - `/v2/updates/trees` HTTP POST and websocket GET endpoints
  - `GetActiveContractsRequest` will be altered in 3.5, it is used in
    - `/v2/state/active-contracts` HTTP POST and websocket GET endpoints
- The configuration for the removed Ledger API transaction tree stream related methods have been removed as well
  ```
  canton.participants.<participant>.ledger-api.index-service.transaction-tree-streams.*
  ```
- gRPC and JSON API payloads for create events (create arguments, interface views and keys) are now
  rendered using the normal form of the types defined in `CreatedEvent.representative_package_id`.
  This should be transparent on the returned values, with the exception of the record-ids in Ledger API verbose
  rendering (`verbose = true`) which now effectively reflect the representative package id as well.
- `VettedPackagesRef` have to refer to only one package if used to vet packages. If they refer to zero packages while unvetting, we log to debug.
- Aligned new Sequencer `GetTime` gRPC API with established conventions
- Topology dispatching errors are now logged at WARN level (instead of ERROR).
- Party allocation and tx generation is now supported on Ledger API.
- BREAKING: minor breaking console change: the BaseResult.transactionHash type has been changed from ByteString to TxHash. The Admin API itself remained unchanged.
- **BREAKING**: `timeProofFreshnessProportion` has been removed

## until 2025-09-24 (Exclusive)
- Add new Ledger API endpoints to improve UX of package vetting:

  1. `ListVettedPackages` in `package_service.proto`, to easily list which
     packages are vetted.
  2. `UpdateVettedPackages` in `package_management_service.proto`, to easily vet
     and unvet packages.

- Modify `UploadDarFileRequest` in `package_management_service.proto` to take a
  `vetting_change` attribute, which specifies whether the uploaded DAR should be
  vetted or not.

- Added a new `GetTime` gRPC endpoint to the sequencer API that returns a "current" sequencing time.
- All JSON API v1 endpoints `/v1/*` have been removed.
- The legacy gRPC Ledger API method `CommandService.SubmitAndWaitForTransactionTree` has been removed. The JSON version
  of this request `/v2/commands/submit-and-wait-for-transaction-tree` continues to be supported in 3.4, but will be
  removed in 3.5.
- The legacy gRPC Ledger API methods in the `UpdateService` have been removed.
  - `GetUpdateTrees`
  - `GetTransactionTreeByOffset`
  - `GetTransactionTreeById`
  - `GetTransactionByOffset`
  - `GetTransactionById`
- The JSON versions of the removed `UpdateService` requests continue to be supported in 3.4, but will be removed in 3.5.
  - `/v2/updates/trees`
  - `/v2/updates/transaction-tree-by-offset`
  - `/v2/updates/transaction-tree-by-id`
  - `/v2/updates/transaction-by-offset`
  - `/v2/updates/transaction-by-id`
- `ParticipantRepairService.ImportAcs` is updated to accommodate new smart-contract upgrading semantics that are introduced in
  in Canton 3.4. More specifically:
  - **BREAKING** The `ImportAcsRequest.contract_id_suffix_recomputation_mode` is renamed to `ImportAcsRequest.contract_import_mode`
    and `ContractIdSuffixRecomputationMode` enum is renamed to `ImportAcsRequest.ContractImportMode` to better reflect its purpose.
    Upon import, contracts can be fully validated (including contract-id suffix recomputation).
  - `ImportAcsRequest.representative_package_id_override` is introduced to allow overriding the original package id of the imported contracts.
    This allows the target participant to use a compatible alternative package for the contract
    without needing to upload original contracts packages.

- **BREAKING**: `topologyChangeDelay` has been moved from `DynamicSynchronizerParameters` to `StaticSynchronizerParameters` and cannot be changed
  on a physical synchronizer.

- **BREAKING**: `reassignmentTimeProofFreshnessProportion` has been moved to nested location `reassignmentsConfig.timeProofFreshnessProportion`

- Deduplication references added to Ledger API DB, giving performance improvement for ACS retrievals in presence of a lot of archived contracts.
  - New participant config parameter `active-contracts-service-streams-config.id-filter-query-parallelism` is added, controlling the
    introduced parallel processing stage filtering IDs (default: 2) during the Ledger API client streaming.
  - New participant config parameter `indexer-config.db-prepare-parallelism` is added, controlling the introduced parallel stage processing
    stage computing deactivation references during indexing.

## until 2025-09-17 (Exclusive)
- The participant admin workflows have been renamed

  1. from `AdminWorkflows` to `canton-builtin-admin-workflow-ping` as part of which the legacy
     `AdminWorkflows/Canton/Internal/PartyReplication.daml` has been removed,
  2. from `PartyReplication` to `canton-builtin-admin-workflow-party-replication-alpha`.

  The README files have been updated to explicitly mandate that updates have to be SCU-compliant
  and with instructions on how to test modifications for compliance.

## until 2025-09-10 (Exclusive)
- **Breaking** Moves general, LAPI active contract based, `ExportAcs` endpoint from `party_management_service.proto`
  to `participant_repair_service.proto`. Note that endpoint does not return retriable error(s) since ACS export is
  defined by the ledger offset.
- **Breaking** Removes `ExportAcsAtTimestamp` endpoint from `party_management_service.proto`.
- Adds a new, party replication focused `ExportPartyAcs` endpoint to `party_management_service.proto`. This endpoint
  finds the correct ledger offset (party activation on the target participant) and excludes active contracts from the
  export which have stakeholders that are already hosted on the target participant (contract duplication prevention).
- Adds the capability to exclude active contracts in the `ExportAcs` and `ImportAcs` endpoints
  (`participant_repair_service.proto`). Exclusion criterion: Any (active) contract that has one or more of a given
  set of parties as a stakeholder will be omitted.
- Add synchronizer_id to Reassignment Ledger API (gRPC and JSON) message. This aligns the Reassignment with the Transaction messages,
  which also hold this property on top level. This also helps clients to make sense of the record time of this update without
  looking into the events themselves.
- **Breaking** Removed the deprecated requesting_parties field from the `GetEventsByContractIdRequest` message in the
  Ledger API. Clients should use the `event_format` field instead, as described in lapi-migration-guide.
- Add `FORCE_FLAG_ALLOW_VET_INCOMPATIBLE_UPGRADES` in topology manager write service. It allows vetting packages
  that are upgrade incompatible.

## until 2025-09-04 (Exclusive)

- Replace an unbounded timeout with a configurable timeout when waiting to observe the submitted topology transactions.
  Additionally, the delay between retries of the topology dispatching loop has been made configurable.
  ```
  participants.participant1.topology.topology-transaction-observation-timeout = 30s // default value
  participants.participant1.topology.broadcast-retry-delay = 10s // default value

  mediators.mediator1.topology.topology-transaction-observation-timeout = 30s // default value
  mediators.mediator1.topology.broadcast-retry-delay = 10s // default value

  sequencers.sequencer1.topology.topology-transaction-observation-timeout = 30s // default value
  sequencers.sequencer1.topology.broadcast-retry-delay = 10s // default value
  ```
- **Breaking** Renamed `AuthenticationTokenManagerConfig#pauseRetries` to `minRetryInterval`.
- **Breaking** Package upgrade validation moved to vetting state change.
  Thus uploading an upgrade-incompatible DAR with vetting disabled is now possible.
  Related error codes changed:
    - `DAR_NOT_VALID_UPGRADE` is renamed `NOT_VALID_UPGRADE_PACKAGE`
    - `KNOWN_DAR_VERSION` is renamed `KNOWN_PACKAGE_VERSION`
## until 2025-08-28 (Exclusive)

- **Breaking** we have removed support for `ecies-hkdf-hmac-sha-256-aes-128-gcm` encryption algorithm specification.
- **Breaking** `OwnerToKeyMapping` (OTK) and `PartyToKeyMapping` (PTK) are now restricted to a maximum of 20 keys.
  These bounds are enforced in the factory methods `create` and `tryCreate`. Additionally, the constructor of `OwnerToKeyMapping`
  has been made private and OTK values must be created via the factory methods.
- **Breaking** Alongside the aforementioned change to OTK and PTK, the console commands have been changed to accept the factory
  method parameters instead of OTK and PTK values directly. This is in line with the `propose` methods for other mappings as well.
- **Breaking** The `SequencerConnections` protobuf structure takes a new parameter `sequencer_liveness_margin` that
  determines the number of extra subscriptions to maintain beyond `sequencer_trust_threshold` in order to ensure
  liveness. This parameter is only used when the new sequencer connection pool is enabled, and is ignored otherwise.
- **Breaking** The `SequencerConnections` class' public constructor `many(...)` takes accordingly a new parameter
  `sequencerLivenessMargin`.
- **Breaking** In non-verbose mode event rendering of Ledger API queries, trailing Optional record field that are not populated
  are no longer included in the Record representation. The reason for this is so that the same structural representation
  is produced independently of the package version that was used to enrich it.

## until 2025-08-21 (Exclusive)

- **Breaking** In verbose mode reporting, record values will no longer contain trailing Optional fields that
  have a value of [None](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153).
  The reason for this is so that the same structural representation is produced independently of the
  package version that was used to enrich it.
- Synchronizer owners are implicitly authorized to REMOVE any topology transaction on the synchronizer, even if they
  are not the "normal" authorizers. As a consequence, the unused topology mapping `PurgeTopologyTransaction` has been removed
  from the code base.
- The HTTP connection timeout is configurable in the Ledger JSON API via
  `canton.participants.<participant-id>.http-ledger-api.server.request-timeout=<duration>`. Configure this value to allow
  more complex Ledger API requests to complete (e.g. `/state/active-contracts`). The default value is 20 seconds.

## until 2025-08-13 (Exclusive)

- **Breaking** In `com/digitalasset/canton/admin/participant/v30/party_management_service.proto` used by Online Party Replication,
  the `GetAddPartyStatusResponse.Status` enum has been extended with a new value, `FullyReplicatedAcs` shifting the
  Protobuf ordinals for some existing status enum values.
- Endpoints and console commands LocatePruningTimestamp are renamed to FindPruningTimestamp.

- **Breaking** Transactions with transient events for which there was an intersection between submitters
  and querying parties were previously exposed as transactions with empty events in AcsDelta shape. Those transactions
  will no longer be exposed at all in the AcsDelta shape. As before, they will still be exposed in the LedgerEffects
  shape.

- **Breaking** Participant divulgence (i.e. if there is a party divulgence, and none of the stakeholders of the divulged contracts
  are hosted on the participant) is no longer performed in Active Contracts Service and AcsDelta transaction shapes.

- The pruning of divulged contracts has changed. Previously, the flag `prune_all_divulged_contracts` of the `PruneRequest`
  was used to prune all the immediately divulged contracts. The divulged contracts will be pruned along with the
  deactivated contracts during the regular pruning process. Thus, the flag `prune_all_divulged_contracts` is a no-op.

## until 2025-08-06 (Exclusive)

- **Breaking** Online Party Replication protocol messages in `com.digitalasset.canton.participant.protocol.v30.party_replication.proto`
  modified:

  Under `PartyReplicationSourceParticipantMessage`, `SourceParticipantIsReady` removed and `data_or_status`
  proto ordinals changed.

  Under `PartyReplicationTargetParticipantMessage`, `Initialize` added, `SendAcsSnapshotUpTo` renamed to `SendAcsUpTo`,
  and `instruction` proto ordinals changed.

- ListConnectedSynchronizersResult.synchronizerId renamed to physicalSynchronizerId

- Add non-standard configuration for `canton.participants.<participant-id>.features.snapshot-dir`. This determines the
  directory for storing snapshotting data.

- Created and exercised events in AcsDelta transactions now include a flag indicating
  whether this event would be part of the respective ACS_DELTA shaped stream, and should therefore be considered
  when tracking contract activeness on the client-side. This way clients with LedgerEffects subscriptions are enabled
  to track contract lifecycle. The Java bindings and the JSON api messages have been extended accordingly.

- Default postgres version is now 17 (instead of 14)
- Default Java version to run the tests is 21 (instead of 17).
  Compilation target is still 17 to continue running on JRE 17.

## until 2025-07-30 (Exclusive)

- Message `Synchronizer` in `com.digitalasset.canton.topology.admin.v30.common.proto` moved from `StoreId` to top level
- **Breaking** The configuration for the admin-token based authorization has changed.

  Previously, the setting of the admin token string was possible through Ledger API or Admin API configuration like so:

  ```
  -C canton.participants.<participant-id>.ledger-api.admin-token="<your-token>"
  or
  -C canton.participants.<participant-id>.admin-api.admin-token="<your-token>"
  ```

  Now, it is set through

  ```
  -C canton.participants.<participant-id>.ledger-api.admin-token-config.fixed-admin-token="<your-token>"
  or
  -C canton.participants.<participant-id>.admin-api.admin-token-config.fixed-admin-token="<your-token>"
  ```

  Other parameters of the admin token can also be set through the `AdminTokenConfig` configuration case class as seen below:

  ```
  final case class AdminTokenConfig(
    fixedAdminToken: Option[String] = None,
    adminTokenDuration: PositiveFiniteDuration = AdminTokenConfig.DefaultAdminTokenDuration,
    actAsAnyPartyClaim: Boolean = true,
    adminClaim: Boolean = true,
  )
  ```

  The `fixedAdminToken` as `adminToken` did before, defines a token that is valid throughout the entire canton process
  lifespan. It is only meant for testing purposes and should not be used in production.

  Other admin-tokens will be generated and rotated periodically for internal usage (e.g. in the console).
  They are invisible from the outside. Each admin-token of these is valid for the defined `adminTokenDuration`.
  The half of the token duration is used as the rotation interval, after which a new admin-token is generated
  (if needed) and used. The default value for the token duration is 5 minutes.

  Setting the `actAsAnyPartyClaim` to `true` allows usage of the admin-token to authorize acting-as and reading-as
  any party in the participant. Similarly, setting the `adminClaim` to `true` allows usage of the admin-token to
  authorize any admin level operation in the participant. As setting these parameters to `true` is consistent with
  the past system behavior, it is the default value for now. We are planning to change them both to `false` in the
  3.4 release to increase default system security. When that happens, the admin-token by default will only be strong
  enough to issue pings.
- Previously, ledger API queries using filters allowed interface or template identifiers to be defined in either
  `package-name` or `package-id` format within the event format (specifically, via the `package_id` field in the `Identifier`
  message of `InterfaceFilter` and `TemplateFilter` in `CumulativeFilter`).
  However, the `package-id` format is now deprecated and will be removed in future releases. A warning message will be
  logged if it is used, as the system internally converts it to the corresponding `package-name` format and resolves
  the query by `package-name` and not by `package-id`.

## until 2025-07-23 (Exclusive)
- OTLP trace export configuration has been extended with several new parameters allowing connection to OTLP servers,
  which require more elaborate set-up:
    - `trustCollectionPath` should point to a valid CA certificate file. When selected a TLS connection
      is created instead of an open-text one.
    - `additionalHeaders` allows specifying key-value pairs that are added to the HTTP2 headers on all trace exporting
      calls to the OTLP server.
    - `timeout` sets the maximum time to wait for the collector to process an exported batch of spans.
      If unset, defaults to 10s.
    - `connectTimeout` sets the maximum time to wait for new connections to be established. If unset, defaults to 10s.
- Bugfix: Corrected HTTP method for the JSON Ledger API endpoint `interactive-submission/preferred-packages` from GET to POST.
- GetConnectedSynchronizers command now can be accessed either with ReadAs or Admin or IDP admin permissions. As a
  result, the proto command also now has an identityProviderId field.

## until 2025-07-16 (Exclusive)
- **Breaking** The `ledger_api.parties.allocate` console command expect the SynchronizerId as an `Option[SynchhronizerId]` instead of a `String`.
- **Breaking** The `synchronizers.id_of` console command returns now the `SynchronizerId` instead of a `PhysicalSynchronizerId`. Another command `synchronizers.physical_id_of` has been added to return the `PhysicalSynchronizerId`.

- The package dependency resolver, which is used in various topology state checks and transaction processing is improved as follows:
    - The underlying cache is now configurable via `canton.parameters.general.caching.package-dependency-cache`.
      By default, the cache is size-bounded at 10000 entries and a 15-minutes expiry-after-access eviction policy.
    - The parallelism of the DB package fetch loader used in the package dependency cache
      is bounded by the `canton.parameters.general.batching.parallelism` config parameter, which defaults to 8.
- **Breaking** Renamed mediator scan to mediator inspection for both the commands and the admin API service. Renamed the inspection service gRPC of the participant into ParticipantInspectionService to differentiate from the mediator one.

## Until 2025-07-09 (Exclusive)
- Sequencer API endpoint `SequencerService.SubscribeV2` has been renamed to `SequencerService.Subscribe`.
- The limit in the config option `canton.sequencers.sequencer.parameters.sequencer-api-limits` has been renamed accordingly:
  `"com.digitalasset.canton.sequencer.api.v30.SequencerService/Subscribe" : 1000`



## Until 2025-07-09 (Exclusive)

- Added new limits for the number of open streams. This allows to limit the number of
  open streams on the API
  ```
  canton.sequencers.sequencer.parameters.sequencer-api-limits = {
    "com.digitalasset.canton.sequencer.api.v30.SequencerService/DownloadTopologyStateForInit" : 10,
    "com.digitalasset.canton.sequencer.api.v30.SequencerService/SubscribeV2" : 1000,
  }
  ```
- Authorization of the calls made by the IDP Admins has been tightened. It is no longer possible for them to grant
  rights to parties which are in other IDPs or in no IDP. This effectively enforces keeping the IDP Admins within
  their respective IDP boxes. Participant Admins can still grant the rights that cross the IDP box boundaries e.g.
  A User in IDP A can be given right to a party IDP B.

## Until 2025-07-02 (Exclusive)

- Adds new gRPC endpoint `GetHighestOffsetByTimestamp` (and console command `find_highest_offset_by_timestamp`) that
  for a given timestamp, finds the highest ledger offset among all events that have record time <= timestamp. This is a
  backward-compatible change, because it's an addition only. It's useful for party replication / major upgrade.

## Until 2025-06-25 (Exclusive)
- [Breaking Change] Updated the `key-validity-duration`, `cut-off-duration`, and `key-eviction-period` parameters in the `crypto.kms.session-signing-keys` configuration to accept only positive durations (e.g., 30m, 5s).
- JSON Ledger API: `prefetchContractKeys` added to `JsCommands` and `JsPrepareSubmissionRequest`
- JSON Ledger API: fixed openapi documentation for: `Completion/Completion1` (status property), `ParticipantAuthorizationAdded`, `ParticipantAuthorizationChanged`,`ParticipantAuthorizationRevoked`
- Ledger API: the existing `InteractiveSubmissionService.GetPreferredPackageVersion` (gRPC) or `interactive-submission/preferred-package-version` (JSON) functionality is superseeded by a new endpoint pair:
  - gRPC: `InteractiveSubmissionService.GetPackagePreferences`
  - JSON: `interactive-submission/package-preferences`

  The existing endpoints are deprecated but preserved for backwards compatibility.
- Contract arguments for Created events are now always populated for both LedgerEffects and AcsDelta shaped events if
    - there is a party in the filter that is in the witness parties of the event or
    - a party-wildcard filter is defined.

## Until 2025-06-18 (Exclusive)
- Changed the protobuf definition of the admin API `StoreId.Synchronizer` from just having a `string id` field to the following:
```
    message Synchronizer {
      oneof kind {
        string logical = 1;
        string physical = 2;
      }
    }
```
- Some console commands now take `TopologyStoreId.Synchronizer` instead of `SynchronizerId` as parameter.
  This should be non-breaking,because there are implicit conversions from `SynchronizerId` and `PhysicalSynchronizerId` to `TopologyStoreId.Synchronizer`.

## Until 2025-06-11 (Exclusive)
- JSON Ledger API: added `authenticated-user` endpoint to get the current user.

## Until 2025-05-21 (Exclusive)
- The `PartyToParticipant` topology mapping's `HostingParticipant` now has an optional, empty `Onboarding` message
  for use with Online Party Replication and the `PartyManagementService.AddPartyAsync` endpoint.
- Configuring session signing keys (`SessionSigningKeysConfig`) is now only possible through `KmsConfig`,
  as this feature is supported exclusively by KMS providers
  (`canton.participants.<participant>.crypto.kms.session-signing-keys`). Session signing keys are now enabled by
  default.
- Add configuration for the size of the inbound metadata on the Ledger API. Changing this value allows
  the server to accept larger JWT tokens.
`canton.participants.participant.ledger-api.max-inbound-metadata-size=10240`

## Until 2025-06-04 (Exclusive)
- **Breaking** The console command `connect_local_bft` takes now a list of `SequencerReference` instead of a `NonEmpty[Map[SequencerAlias, SequencerReference]]`
- Console command - A new console command `connect_bft` has been added to connect by url to Decentralized Sequencers
-
## Until 2025-05-14 (Exclusive)
- JSON - changes in openapi (`Any` renamed as `ProtoAny`, `Event1` renamed to `TopologyEvent` and fixed, fixed `Field`, `FieldMask`,`JsReassignmentEvent` mappings.

- SynchronizerConnectivityService.GetSynchronizerIdResponse.synchronizer_id changed to physical_synchronizer_id
- SequencerConnectService.GetSynchronizerIdResponse.synchronizer_id changed to physical_synchronizer_id
- MediatorStatusService.MediatorStatusResponse.MediatorStatusResponseStatus.synchronizer_id changed to physical_synchronizer_id
- SequencerStatusService.SequencerStatusResponse.SequencerStatusResponseStatus.synchronizer_id changed to physical_synchronizer_id

- Submission time is now called preparation time:
    - The field `submission_time` for interactive submissions is now called `preparation_time`.
    - The dynamic domain parameter `submission_time_record_time_tolerance` is now called `preparation_time_record_time_tolerance`.
    - The error codes `LOCAL_VERDICT_SUBMISSION_TIME_OUT_OF_BOUND` and `TOPOLOGY_INCREASE_OF_SUBMISSION_TIME_TOLERANCE` are now called `LOCAL_VERDICT_PREPARATION_TIME_OUT_OF_BOUND` and `TOPOLOGY_INCREASE_OF_PREPARATION_TIME_TOLERANCE`.
    - The console commands `set_submission_time_record_time_tolerance` is now called `set_preparation_time_record_time_tolerance`.

## Until 2025-05-07 (Exclusive)
- The VettedPackage validFrom and validUntil fields have been renamed to validFromInclusive and validFromExclusive.

## Until 2025-04-30 (Exclusive)
- JSON API - fixed openapi documentation for maps: (`eventsById`,`filtersByParty`).

### Changed return values in the console `grant` and `revoke` commands
The console commands `ledger_api.users.rights.grant` and `ledger_api.users.rights.revoke`
have been changed to return the complete state of current rights assigned to a user instead of
the "delta" induced by the command. The previous behavior was counterintuitive and was a source
of confusion that resulted in support tickets.

### BREAKING CHANGE: Per-synchronizer party allocation
Console commands and API endpoints for allocating/enabling and removing/disabling parties now operate on a per-synchronizer basis.
This means that party allocations must be done explicitly for each synchronizer, and that the participant
must be connected to each synchronizer at the time of enabling or disabling the party.

The console commands `participant.parties.enable` and `participant.parties.disable` have a new parameter `synchronizer: Option[SynchronizerAlias]`
that specifies on which synchronizer the party should be enabled or disabled. The parameter can be "omitted" or set to `None`, if the participant
is connected to only one synchronizer. The parameter `waitForSynchronizer: SynchronizerChoice` has been removed.

The console command `participant.ledger_api.parties.allocate` has a new parameter `synchronizer_id` for specifying the target synchronizer for the party allocation.
Similar to the parameter for the other console commands, this parameter can be omitted if the participant is connected to only one synchronizer.

The Ledger API request `PartyManagementService.AllocatePartyRequest` now has a new field `string synchronizer_id` for specifying the target synchronizer of the party allocation.
Similar to the parameter for the console commands, this parameter can be omitted if the participant is connected to only a one synchronizer.

If the synchronizer parameter is not specified and the participant is connected to multiple synchronizers, the request fails with the error `PARTY_ALLOCATION_CANNOT_DETERMINE_SYNCHRONIZER`.
If the participant is not connected to any synchronizer, the request fails with the error `PARTY_ALLOCATION_WITHOUT_CONNECTED_SYNCHRONIZER`.

The authorized store can still be used to store `PartyToParticipant` topology transactions, but users are discouraged from doing so.

### Canton Console commands for universal streams api

- The SubscribeTrees and SubscribeFlat LedgerApiCommands were removed. The SubscribeUpdates should be used instead.
- The SubmitAndWaitTransactionTree LedgerApiCommand was removed. The SubmitAndWaitTransaction should be used instead.
- The GetTransactionById and GetTransactionByOffset LedgerApiCommands were removed. The GetUpdateById should be used instead.
- The following canton console commands have been removed:
  - ledger_api.updates.{trees, flat}
  - ledger_api.updates.{trees_with_tx_filter, flat_with_tx_filter}
  - ledger_api.updates.{subscribe_trees, subscribe_flat}
  - ledger_api.updates.{by_id, by_offset}
  - ledger_api.commands.submit_flat
  - ledger_api.javaapi.updates.{trees, flat}
  - ledger_api.javaapi.updates.flat_with_tx_filter
  - ledger_api.javaapi.commands.submit_flat
- The following canton console commands have been added:
  - ledger_api.updates.updates
  - ledger_api.updates.{transactions, reassignments, topology_transactions}
  - ledger_api.updates.transactions_with_tx_format
  - ledger_api.updates.subscribe_updates
  - ledger_api.javaapi.updates.transactions
  - ledger_api.javaapi.updates.transactions_with_tx_format
- For more info on how to migrate follow the migration guide (console-commands-migration-guide.rst)

## Until 2025-04-23 (Exclusive)
- The error code `ABORTED_DUE_TO_SHUTDOWN` is now used instead of the (duplicate) error code `SERVER_IS_SHUTTING_DOWN` that was previously used.

- JSON API - changed encoding for protobuf based enums.
Following types are now encoded as strings:

    - `HashingSchemeVersion`,
    - `PackageStatus`,
    - `ParticipantPermission`,
    - `SigningAlgorithmSpec`,
    - `SignatureFormat`,
    - `TransactionShape`,

- Canton console - ledger_api changed slightly:

    - `submit_assign`, `submit_unassign` and `submit_reassign` changed: the waitForParticipants removed as these
    endpoints now use the same synchronization mechanics as the transaction submission endpoints. Also the timeout
    field became optional: allowing to bypass synchronization if needed.
    - `submit_assign` and `submit_unassign` have the eventFormat parameter removed, and `submit_assign_with_format`
    and `submit_unassign_with_format` endpoints introduced to provide full functionality with the compromise that the
    result can be empty.

- A default value is provided for the ``transaction_format`` field inside of ``SubmitAndWaitForTransactionRequest``.
  You can now omit this field in both grpc and json requests, and get behavior consistent with the 3.2 version of
 Canton. This means you will receive a flat transaction with event visibility dictated by all ``act_as`` and ``read_as`` parties.
