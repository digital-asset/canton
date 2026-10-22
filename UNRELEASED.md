# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### CantonBFT
- Fixed: the P2P connection direction was incorrectly reported in metrics and console/admin endpoints in some circumstances.
- Logs of the dissemination stage of the ordering protocol have been improved for easier troubleshooting.

### Addition of protocol version 37, removal of protocol version 34 and 35.
Protocol versions 34 and 35 are not supported anymore. This release supports protocol versions 36 and 37.

### Protocol version downgrades via LSU are forbidden from protocol version 37 on
Logical synchronizer upgrades from a synchronizer running protocol version 37 or higher to a
successor with a lower protocol version are now rejected. Both the topology manager and the topology
processor reject such `LsuAnnouncement` topology mappings with the error
`TOPOLOGY_INVALID_SUCCESSOR_PHYSICAL_SYNCHRONIZER_ID`.

### Multi-sync GA

Starting with protocol version 37:
- The `EnableMultiSynchronizer` feature flag is not needed to enable multi-synchronizer.
- Any stakeholder can submit the assignment as soon as the unassignment is completed; the `assignment_exclusivity` field of unassigned events is not set, and the synchronizer parameter `assignmentExclusivityTimeout` has no effect.
- New participant configuration `canton.participants.<participant>.parameters.reassignments-config.enable-automatic-assignment` (default `true`). When set to `true`, the participant tries to complete the reassignments initiated by other participants by submitting their assignment, and upon reconnection to a synchronizer it tries to complete its pending reassignments.
- New participant configuration `canton.participants.<participant>.parameters.reassignments-config.automatic-assignment-delay` (default `60s`). The automatic assignment is submitted after a delay drawn between this value and twice this value, counted from the sequencing time of the unassignment on the local clock.

#### Protocol support for assignments with source and target validation packages
Prior to this protocol version 37 the creating package of the contract was used for contract authentication during contract assignment.
From protocol version 37 separate source and target validation packages can be specified at the protocol level.
To be valid these validation packages must be upgradable packages for the contract and be vetted on the respective synchronizer.

### New `v31` topology transactions with party hosting states, and party freeze, from protocol version 37

**Do not use the Nominated and Freezing hosting states until a future 3.7 release announces their support.**
They exist for Hybrid Party Replication (HyPR), which this release does not include: There is no supported
flow that takes a party through them. A Freezing entry, set with `participantStates` or in a `v31`
transaction, blocks every request that involves the party on the synchronizer until the entry is removed.

From protocol version 37, topology transactions can use the new `v31` `TopologyTransaction`
(`com.digitalasset.canton.protocol.v31`). Its `PartyToParticipant` mapping gives each hosting participant a
hosting state instead of the onboarding flag; all other mappings are unchanged. Synchronizers on protocol
version 37 also accept `v30` topology transactions: a `v30` entry is Onboarding if its onboarding flag is
set, and Active otherwise.
- Clients that decode topology transactions must support `v31` on protocol version 37:
  - The Ledger API `GenerateExternalPartyTopology` returns `v31` transactions for a synchronizer on protocol
    version 37.
  - The topology admin API `ListPartyToParticipant` sets the `v31` item for `v31` transactions and the `v30`
    item for `v30` transactions.
- Hosting states, in their forward order:
  - Nominated: added by the party; the participant does not host the party yet.
  - Freezing: the participant does not host the party yet, and the party is frozen on the synchronizer.
  - Onboarding: the participant hosts the party, like an entry with the onboarding flag in `v30`.
  - Active: the participant hosts the party.
- The party alone can add a participant as Nominated; any other addition needs the party and the participant,
  as before. Moving an entry forward needs only that participant's authorization, and states can be skipped.
  Moving an entry backward is rejected. On protocol version 37 these rules also apply to `v30` transactions,
  so setting the onboarding flag of an Active entry is rejected.
- While any entry of a party is Freezing, the party has no hosting participants on that synchronizer:
  - Transactions and reassignments that involve the party are rejected there. A submission fails with the new
    error code `FROZEN_PARTIES` if the freeze is the only reason it cannot be routed. The code has the same
    category as `UNKNOWN_SUBMITTERS` and `UNKNOWN_INFORMEES`.
  - Reads of the party's contracts and its transactions on other synchronizers are not affected. The Ledger API
    `GetConnectedSynchronizers` leaves out the synchronizer on which the party is frozen.
- Entries with Confirmation or Submission permission that are not Active yet lower the party's confirmation
  threshold to `max(1, min(threshold, max(numConfirming, threshold) - numConfirmingPending))`, where
  `numConfirming` counts these entries in any state and `numConfirmingPending` those that are not Active.
- Ledger API topology events: `ParticipantAuthorizationOnboarding` when an entry becomes Onboarding,
  `ParticipantAuthorizationAdded` when it becomes Active, and `ParticipantAuthorizationRevoked` when an
  Onboarding or Active entry is removed. Nominated and Freezing entries emit no events.
- Party management:
  - `party_to_participant_mappings.propose` and `propose_delta` have a new `participantStates` parameter to set
    hosting states. With `propose_delta`, participants that are not in `adds`, and Nominated and Freezing
    participants, keep their state; on protocol version 36 and for the authorized store, it behaves as before.
  - The Ledger API `AllocateParty` fails with `HOSTING_STATE_PREVENTS_PARTY_ALLOCATION`, and `parties.enable`
    fails, if this participant's entry is Nominated or Freezing. The declarative API leaves such parties
    untouched.

### New collection size limits from protocol version 37

Starting with protocol version 37, the following collections are bounded. Messages exceeding a limit are rejected.

| Field                                                               | Default limit | How to configure |
|---------------------------------------------------------------------|---------------|---|
| Signatures per envelope                                             | 1000          | Static synchronizer parameter `synchronizer_limits.transaction_protocol_limits.max_signatures_per_envelope` |
| Recipients per envelope                                             | 10000         | Static synchronizer parameter `synchronizer_limits.transaction_protocol_limits.max_recipients_per_envelope` |
| Confirming parties per quorum                                       | 100 000 000   | Static synchronizer parameter `synchronizer_limits.transaction_protocol_limits.max_quorum_size` |
| Signing keys per entity (`OwnerToKeyMapping`, `PartyToKeyMapping`)  | 20            | Static synchronizer parameter `synchronizer_limits.topology_limits.max_signing_keys_per_entity` |
| Owners of a `DecentralizedNamespaceDefinition`                      | 100           | Static synchronizer parameter `synchronizer_limits.topology_limits.max_owners_per_decentralized_namespace` |
| Hosting participants per party (`PartyToParticipant`)               | 100           | Static synchronizer parameter `synchronizer_limits.topology_limits.max_hosting_participants_per_party` |
| ACS commitment size in bytes                                        | 10 KB         | Static synchronizer parameter `synchronizer_limits.acs_commitment_limits.max_acs_commitment_size_bytes` |
| ACS commitment summary size in bytes                                | 1 MB          | Static synchronizer parameter `synchronizer_limits.acs_commitment_limits.max_acs_commitment_summary_size_bytes` |
| Addressed counter-participants in an ACS commitment summary         | 10000         | Static synchronizer parameter `synchronizer_limits.acs_commitment_limits.max_addressed_counterparticipants` |
| Unsent digests in an ACS commitment summary                         | 10000         | Static synchronizer parameter `synchronizer_limits.acs_commitment_limits.max_unsent_digests` |
| ACS commitment digest size in bytes                                 | 50            | Not configurable |
| Mappings in a `NamespaceDelegation` restricted to specific mappings | 50            | Not configurable |
| Feature flags in a `SynchronizerTrustCertificate`                   | 50            | Not configurable |

Note that the default limits only apply when used through the console.
When using the API directly to bootstrap a synchronizer on PV37, the latest protobuf version (32) of the `StaticSynchronizerParameters` message must be used, which contains the new limit fields.
Values for those new limits must then be set explicitly.

### CantonBFT
- P2P: TCP- and HTTP2-level metrics.
- Added metrics for the time ordered blocks wait before delivery while lower-numbered blocks are still pending.
- Starting with PV37, state transfer responses can carry multiple commit certificates. This allows for more efficient
  catch-up and onboarding.
- Simplify batch look up query by removing epoch 0 special case.
- Request pre-prepare via retransmissions if a weak quorum of prepares is reached without having the pre-prepare. this is an important performance improvement when there is high latency between some nodes and pre-prepares are often delayed or lost.
- Added metrics for pre-prepares accepted coming from retransmissions
- Enables patience-based dissemination retries also for batches that have reached the minimum dissemination
  threshold, but neither have been disseminated to all nodes yet and nor have become part of a block proposal yet.
- Fix BatchId hashing to be consistent across protocol version.

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

### Breaking: removal of the deprecated sequencer onboarding state APIs
The sequencer onboarding state endpoints that were deprecated in favour of their `V2` counterparts have been removed,
together with the proto messages they used. They would give an incomplete view of the sequencer state and were not
compatible with the new onboarding process.

Removed from `com.digitalasset.canton.sequencer.admin.v30`:
- `SequencerAdministrationService.OnboardingState` (use `OnboardingStateV2`).
- `SequencerInitializationService.InitializeSequencerFromOnboardingState` (use `InitializeSequencerFromOnboardingStateV2`).

The corresponding console commands have been removed as well:
- `sequencer.setup.onboarding_state_for_sequencer` and `sequencer.setup.onboarding_state_at_timestamp`
  (use `onboarding_state_for_sequencerV2` / `onboarding_state_at_timestampV2`).
- `sequencer.setup.assign_from_onboarding_state` (use `assign_from_onboarding_stateV2`).

### New `v31` sequencer admin protos
New `v31` versions of `SequencerSnapshot`, `SequencerPruningStatus`, `SequencerMemberStatus` and
`OnboardingStateForSequencerV2` have been added in `com.digitalasset.canton.sequencer.admin.v31`. They can represent
members for which traffic was purchased before their registration. Sequencer snapshots and onboarding states use them
from protocol version 37 onwards. Earlier protocol versions keep using the `v30` messages and are not affected.

`PruningStatusResponse` keeps using the `v30` messages, and therefore omits the members that are not registered yet:
those that only had traffic purchased for them, and those disabled before their registration. These members have no
events and do not affect pruning.

### Collection Size Limit Metrics For Transactions and Transaction Views During Submission
During submission (i.e. phase 1), the engine emits the following limit metrics for transactions (with a `limit` label
for the currently configured threshold limit):
- `daml.participant.transaction-node.acting-parties`
- `daml.participant.transaction-node.choice-authorizers`
- `daml.participant.transaction-node.choice-observers`
- `daml.participant.transaction-node.external-call-result-size`
- `daml.participant.transaction-node.external-call-results`
- `daml.participant.transaction-node.input-contract-count`
- `daml.participant.transaction-node.key-maintainers`
- `daml.participant.transaction-node.lf-value`
- `daml.participant.transaction-node.node-children-size`
- `daml.participant.transaction-node.node-count`
- `daml.participant.transaction-node.root-node-count`
- `daml.participant.transaction-node.signatories`
- `daml.participant.transaction-node.stakeholders`
- `daml.participant.transaction-node.total-informees`

and the following limit metrics for transaction views:
- `daml.participant.sync.protocol-messages.transaction-view.sub-views`
- `daml.participant.sync.protocol-messages.transaction-view.tree-depth`

### Breaking: Repair commands represent contracts depending on their reassignment counter

#### Background

ACS imports and the repair `add`/`purge` commands used either Created/Archive events or
Assigned/Unassigned events for all contracts, depending on `enable-all-ledger-api-reassignments`.
Created/Archive events cannot carry a non-zero reassignment counter, so such contracts could not be
imported when flag was set to false.

#### Specific Changes

The meaning of the participant parameter
`canton.participants.<participant>.parameters.enable-all-ledger-api-reassignments` has changed:
- `true`: unchanged, all contracts are represented as Assigned (import, add) or Unassigned (purge)
  events.
- `false` (default): contracts with a zero reassignment counter are represented as Created (import,
  add) or Archive (purge) events, contracts with a non-zero reassignment counter are represented as
  Assigned (import, add) or Unassigned (purge) events, preserving the reassignment counter.
  Previously, all contracts were represented as Created/Archive events and the import of contracts
  with a non-zero reassignment counter was rejected.

#### Impact and Migration

Contracts with a non-zero reassignment counter can now be imported without enabling
`enable-all-ledger-api-reassignments`. Ledger API consumers must be able to process Assigned and
Unassigned events for such contracts.

### Breaking: Addition of a flag controlling presence of reassignment events in the `StateService`.

#### Background

Incomplete reassignments (`IncompleteAssigned` and `IncompleteUnassigned` events) are only relevant
to clients that work with contracts reassigned between multiple synchronizers. Most clients only
need the active contracts. Until now, the snapshot always included these events. The new flag lets
clients opt in to incomplete reassignments only when they need them.

#### Specific Changes

A flag controlling presence of reassignment events has been added to the `GetActiveContractsRequest`
and `GetActiveContractsPageRequest` of the `StateService`. If the `includeIncompleteReassignments`
flag is not set, the reassignments are excluded from the response stream. JSON version of the api
has been also extended accordingly. For details see the OpenApi specification. As an illustration,
the following request for the `active-contracts-page` endpoint will return only the active contracts
and exclude any reassignments:

  ```
    POST http://localhost:6865/v2/state/active-contracts-page
    Content-Type: application/json
    {
      "activeAtOffset": 1000000,
      "eventFormat": {...},
      "maxPageSize": 100,
      "pageToken": "",
      "includeIncompleteReassignments": false
    }
  ```

The `GetActiveContracts` stream continuation token and the `GetActiveContractsPage` page token formats
have changed to take the new flag into account, so tokens issued by previous Canton versions are rejected
with `INVALID_CONTINUATION_TOKEN` and `INVALID_ACS_PAGE_TOKEN` respectively.

#### Impact and Migration

This change is backwards incompatible, as the default value of the new flag is `false`,
which means that reassignments are excluded from the response stream. The previous behavior
of including reassignments in the response stream can be restored by setting
the `includeIncompleteReassignments` flag to `true` in the request.
This change in behavior affects all early adopters of the Multi-Synchonizer Alpha feature.

### Minor Improvements
- The topology and offline root key scripts support topology transaction proto version 31 (used from protocol version 37) which is now the default.
  Pass the protocol version with `--protocol-version` to `prepare-cert.sh`, `interactive_submission.py`,
  `interactive_topology_example.py`, and `interactive_topology_example.sh` if the topology transactions should be generated with a lower proto version (e.g., when connected to a synchronizer running protocol version 36).
- `prepare-cert.sh` rejects `--intermediate-delegation` combined with `--root-delegation` or `--delegation-restrictions`.
- Iterative manual pruning over the Ledger API. Scheduled pruning was already iterative based on the `batching-config.max-pruning-batch-size`, which controls the size of the maximum offset range in one iteration. This improvement turned manual pruning to iterative as well, based on the same participant configuration.
  Contract pruning made also iterative as well, controlled by the participant configuration `index-service.contract-pruning-batch-size`, which defines the maximum amount of contracts pruned in one iteration. (Default: 10000)
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
- Added `TopologyStore.findStoredByHashes`, which looks up the latest stored topology transactions for a set of hashes in bounded batches, including rejected ones.
- The submitting node now logs an INFO when a topology transaction sent through the queue-based or store-based topology outbox is sequenced but rejected during topology validation.
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
- Improved the performance of the Ledger API database queries that fetch event sequential ids for updates and ACS streams. The
  `(event_sequential_id, first_per_sequential_id)` index on the Ledger API filter tables is also replaced by a plain index on `event_sequential_id`
  and a partial index on `event_sequential_id` restricted to `first_per_sequential_id`.
- Added documentation generation for some missing metrics on
  `https://docs.canton.network/global-synchronizer/reference/canton-metrics`:
    - daml.sequencer-client.sequencer-connection-pool.connection-health
    - daml.sequencer-client.sequencer-connection-pool.subscription-health
    - daml.sequencer-client.handler.delay-per-connection
    - daml.sequencer-client.handler.last-sequencing-time-micros-per-connection
    - daml.sequencer-client.traffic-control.last-traffic-update
    - daml.sequencer-client.traffic-control.extra-traffic-purchased
    - daml.sequencer-client.traffic-control.extra-traffic-consumed
    - daml.sequencer-client.traffic-control.base-traffic-remainder
    - daml.participant.sync.commitments.counter-participant-latency
    - daml.participant.sync.commitments.latest-matching-status
    - daml.participant.sync.commitments.latest-matching-status-period-end
    - daml.participant.sync.conflict-detection.task-queue
    - daml.participant.sync.request-tracker.task-queue
- Deprecated configuration settings: `canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-34`, `canton.participants.<participant>.features.deprecated.enable-deprecated-parameters-34` and `canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-35`. These 3.6 flags re-enabling the Ledger API endpoints and parameters deprecated in 3.4 and 3.5 have no effect anymore and are ignored when present.
- Added the traffic enforcement admin endpoint `Prune` (JSON `POST /v2/traffic/prune`, console command `ledger_api.traffic.prune`).
  It prunes events and submission correlations, and reports the pruned event and correlation counts separately.
  The timestamp must be at least the participant's max deduplication duration (`canton.participants.<participant>.init.ledger-api.max-deduplication-duration`, default 7 days) in the past, otherwise the request fails with `TRAFFIC_UNSAFE_TO_PRUNE`.
  WARNING: If a submission's correlation is pruned before its completion arrives, the submission is NOT debited.
- Deprecated the traffic enforcement admin endpoint `PruneEvents`, the JSON endpoint `POST /v2/traffic/events/prune` and the console command `ledger_api.traffic.prune_events`. Use `Prune` instead. They will be disabled by default in 3.8 and removed in 3.9.
  Setting `canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-37 = false` (default `true`) disables them earlier, and calls then fail with `DEPRECATED_API_DISABLED`.
- Use the same `currentSnapshotApproximation` and signing timestamp for both the inner message (`ConfirmationResponses` or `ConfirmationResultMessage`) and its enclosing `SubmissionRequest`.

#### New file-based console commands for sequencer initialization and topology snapshots

Added new console commands that stream the state to or from a file, for states larger than 2 GB:
- `node.topology.transactions.export_genesis_state`
- `node.topology.transactions.export_topology_snapshot_to_file`
- `sequencer.setup.export_onboarding_state_for_sequencer`
- `sequencer.setup.export_onboarding_state_at_timestamp`
- `sequencer.setup.import_genesis_state`
- `sequencer.setup.import_onboarding_state`

`node.topology.transactions.import_topology_snapshot_fromV2` supports now files larger than 2 GB.
- Added support to purchase traffic for a participant that is not yet registered with the sequencer.
  This allows for improvements in the onboarding process of participants. It requires protocol
  version 37 or higher; on earlier protocol versions the sequencer rejects such a purchase with
  `TRAFFIC_PURCHASE_FOR_UNREGISTERED_MEMBER_UNSUPPORTED`, because the sequencer snapshot format used
  there cannot represent a member that is still onboarding.
- Indexer pipeline now breaks up heavy transaction to evenly sized batches, where the 'size' is determined by the number of database operations.
- Deprecated the `canton.participants.<participant>.parameters.ledger-api-server.indexer.batching-parallelism` configuration setting, which is no longer supported.
- ConvertRecordTimeToOffset API call was not detecting situation where the requested record time at which there were multiple updates and some of them were already pruned was returning earliest not pruned offset, now the request is rejected with `RECORD_TIME_NOT_FOUND` error code.

#### Source and target validation packages can now be manually overridden.
- The following console commands now take optional `overrideSourceValidationPackageIds` and `overrideTargetValidationPackageIds` arguments:
  - `submit_unassign`
  - `submit_unassign_with_format`
  - `submit_reassign`
  - `submit_unassign_async`
- The `UnassignCommand` java binding also supports these additional fields.
- Fixed documentation comments in Ledger API proto definiton that mentioned usage of package id. This feature was removed in 3.5

### Preview Features
- preview feature

## Bugfixes
- bump da-base-image to 1.0.14
- Update Bouncy Castle to 1.86 to address security vulnerabilities.
- Minor performance fix: time proofs have now a max sequencing timeout of 2 minutes and they no longer create a
  performance regression during synchronizer catch-up. Furthermore, synchronous rejects were not properly
  cleaned up by the sequencer client immediately after the reject was processed, but relied on the timeout logic to pick
  up the requests.
- Fixed the transaction trace shown for unhandled exceptions: when the exception passed through
  a try-catch that did not handle it, the trace started at the choice enclosing the try-catch
  instead of the choice where the exception was thrown.
- Improve error reporting of various admin API endpoints in case of the server being shut down: ABORTED_DUE_TO_SHUTDOWN is now reported in some cases instead of internal errors.
- CantonBFT: ensure topology is always propagated to the availability module, even if this node is blacklisted.
- Minor fix: sequencer acknowledgements are now rejected synchronously whenever the sequencer cannot accept them.
- gRPC server handler logs now carry the request's trace id when a client calls with an empty `TraceContext`, for example the sequencer connection pool's `Authenticate`, `Subscribe` and `DownloadTopologyStateForInit`. The client interceptor uses a default trace id when the caller supplied none, instead of sending no `traceparent`.
- Fix internal error when calling `JoseService.GetJwks` for Ed25519 keys

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
