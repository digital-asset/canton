# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/vCANTON_VERSION). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-CANTON_VERSION.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/CANTON_VERSION/canton/about.html).

INFO: Note that the **"## Until YYYY-MM-DD (Exclusive)" headers**
below should all be Wednesdays to align with the weekly release
schedule, i.e. if you add an entry effective at or after the first
header, prepend the new date header that corresponds to the
Wednesday after your change.

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
