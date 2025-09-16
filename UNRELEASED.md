# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/vCANTON_VERSION). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-CANTON_VERSION.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/CANTON_VERSION/canton/about.html).

INFO: Note that the **"## Until YYYY-MM-DD (Exclusive)" headers**
below should all be Wednesdays to align with the weekly release
schedule, i.e. if you add an entry effective at or after the first
header, prepend the new date header that corresponds to the
Wednesday after your change.

## until 2024-09-11 (Exclusive)
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

## until 2025-09-04 (Exclusive)
- Replace an unbounded timeout with a configurable timeout when waiting to observe the submitted topology tranactions.
  Additionally, the delay between retries of the topology dispatching loop has been made configurable.
  ```
  participants.participant1.topology.topology-transaction-observation-timeout = 30s // default value
  participants.participant1.topology.broadcast-retry-delay = 10s // default value

  mediators.mediator1.topology.topology-transaction-observation-timeout = 30s // default value
  mediators.mediator1.topology.broadcast-retry-delay = 10s // default value

  sequencers.sequencer1.topology.topology-transaction-observation-timeout = 30s // default value
  sequencers.sequencer1.topology.broadcast-retry-delay = 10s // default value
  ```
- Topology dispatching errors are now logged at WARN level (instead of ERROR).
- Party allocation and tx generation is now supported on Ledger API.
- BREAKING: minor breaking console change: the BaseResult.transactionHash type has been changed
  from ByteString to TxHash. The Admin API itself remained unchanged.

## until 2025-08-25 (Exclusive)
- The HTTP connection timeout is configurable in the Ledger JSON API via
  `canton.participants.<participant-id>.http-ledger-api.server.request-timeout=<duration>`. Configure this value to allow
  more complex Ledger API requests to complete (e.g. `/state/active-contracts`). The default value is 20 seconds.

## until 2025-07-23 (Exclusive)
- Bugfix: Corrected HTTP method for the JSON Ledger API endpoint `interactive-submission/preferred-packages` from GET to POST.

## Until 2025-07-16 (Exclusive)
- **Breaking**  The console command `parties.update` has been removed. You can now use the Ledger API command `ledger_api.parties.update` instead.
- **Breaking** Renamed mediator scan to mediator inspection for both the commands and the admin API service. Renamed the inspection service gRPC of the participant into ParticipantInspectionService to differentiate from the mediator one.

## Until 2025-06-27 (Exclusive)
- Added new limits for the number of open streams. This allows to limit the number of
  open streams on the API
  ```
  canton.sequencers.sequencer.parameters.sequencer-api-limits = {
    "com.digitalasset.canton.sequencer.api.v30.SequencerService/DownloadTopologyStateForInit" : 10,
    "com.digitalasset.canton.sequencer.api.v30.SequencerService/SubscribeV2" : 1000,
  }
  ```

## Until 2025-06-25 (Exclusive)
- Adds new gRPC endpoint `GetHighestOffsetByTimestamp` (and console command `find_highest_offset_by_timestamp`) that
  for a given timestamp, finds the highest ledger offset among all events that have record time <= timestamp. This is a
  backward-compatible change, because it's an addition only. It's useful for party replication / major upgrade.
- Ledger API: the existing `InteractiveSubmissionService.GetPreferredPackageVersion` (gRPC) or `interactive-submission/preferred-package-version` (JSON) functionality is superseeded by a new endpoint pair:
    - gRPC: `InteractiveSubmissionService.GetPackagePreferences`
    - JSON: `interactive-submission/package-preferences`

  The existing endpoints are deprecated but preserved for backwards compatibility.

## Until 2025-06-11 (Exclusive)
- Dead parameter `canton.participants.<participant>.http-ledger-api.allow-insecure-tokens` has been removed.

## Until 2025-06-04 (Exclusive)
- **Breaking** The console command `connect_local_bft` takes now a list of `SequencerReference` instead of a `NonEmpty[Map[SequencerAlias, SequencerReference]]`
- Console command - A new console command `connect_bft` has been added to connect by url to Decentralized Sequencers
-
## Until 2025-05-14 (Exclusive)
- JSON - changes in openapi (`Any` renamed as `ProtoAny`, `Event1` renamed to `TopologyEvent` and fixed, fixed `Field`, `FieldMask`,`JsReassignmentEvent` mappings.
- JSON API - fixed openapi documentation for maps: (`eventsById`,`filtersByParty`).
- JSON API - changed encoding for protobuf based enums.
  Following types are now encoded as strings:

    - `HashingSchemeVersion`,
    - `PackageStatus`,
    - `ParticipantPermission`,
    - `SigningAlgorithmSpec`,
    - `SignatureFormat`,
    - `TransactionShape`,

- Submission time is now called preparation time:
  - The field `submission_time` for interactive submissions is now called `preparation_time`.
  - The dynamic domain parameter `submission_time_record_time_tolerance` is now called `preparation_time_record_time_tolerance`.
  - The error codes `LOCAL_VERDICT_SUBMISSION_TIME_OUT_OF_BOUND` and `TOPOLOGY_INCREASE_OF_SUBMISSION_TIME_TOLERANCE` are now called `LOCAL_VERDICT_PREPARATION_TIME_OUT_OF_BOUND` and `TOPOLOGY_INCREASE_OF_PREPARATION_TIME_TOLERANCE`.
  - The console commands `set_submission_time_record_time_tolerance` is now called `set_preparation_time_record_time_tolerance`.
