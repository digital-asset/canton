# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/vCANTON_VERSION). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-CANTON_VERSION.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/CANTON_VERSION/canton/about.html).

## Until 2025-03-05 (Exclusive)
- Fixed slow sequencer shapshot query on the aggregate submission tables in the case when sequencer onboarding state
  is requested much later and there's more data accumulated in the table:
 - DB schema change: added fields and indexes to the aggregate submission tables to speed up the snapshot query.
- A new storage parameter is introduced: `storage.parameters.failed-to-fatal-delay`. This parameter, which defaults to 5 minutes, defines the delay after which a database storage that is continously in a Failed state escalates to Fatal.
  The sequencer liveness health is now changed to use its storage as a fatal dependency, which means that if the storage transitions to Fatal, the sequencer liveness health transitions irrevocably to NOT_SERVING. This allows a monitoring system to detect the situation and restart the node.
  **NOTE** Currently, this parameter is only used by the `DbStorageSingle` component, which is only used by the sequencer.
- Addressing a DAR on the admin api is simplified: Instead of the DAR ID concept, we directly use the main package-id, which is synonymous.
  - Renamed all `darId` arguments to `mainPackageId`

## Until 2025-02-26 (Exclusive)
- The interactive submission service and external signing authorization logic are now always enabled. The following configuration fields must be removed from the participant's configuration:
    - `ledger-api.interactive-submission-service.enabled`
    - `parameters.enable-external-authorization`

## Until 2025-02-19 (Exclusive)
- Added `SequencerConnectionAdministration` to remote mediator instances, accessible e.g. via `mymediator.sequencer_connection.get`

- **BREAKING CHANGE** Remote console sequencer connection config `canton.remote-sequencers.<sequencer>.public-api`
now uses the same TLS option for custom trust store as `admin-api` and `ledger-api` sections:
  - new: `tls.trust-collection-file = <existing-file>` instead of undocumented old: `custom-trust-certificates.pem-file`
  - new: `tls.enabled = true` to use system's default trust store (old: impossible to configure) for all APIs
- The sequencer's `SendAsyncVersioned` RPC returns errors as gRPC status codes instead of a dedicated error message with status OK.
- DarService and Package service on the admin-api have been cleaned up:
  - Before, a DAR was referred through a hash over the zip file. Now, the DAR ID is the main package ID.
  - Renamed all `hash` arguments to `darId`.
  - Added name and version of DAR and package entries to the admin API commands.
  - Renamed the field `source description` to `description` and stored it with the DAR, not the packages.
  - Renamed the command `list_contents` to `get_content` to disambiguate with `list` (both for packages and DARs).
  - Added a new command `packages.list_references` to support listing which DARs are referencing a particular
    package.

- New sequencer connection validation mode `SEQUENCER_CONNECTION_VALIDATON_THRESHOLD_ACTIVE` behaves like `SEQUENCER_CONNECTION_VALIDATON_ACTIVE` except that it fails when the threshold of sequencers is not reached. In Canton 3.2, `SEQUENCER_CONNECTION_VALIDATON_THRESHOLD_ACTIVE` was called `STRICT_ACTIVE`.

- **BREAKING CHANGE** Renamed the `filter_store` parameter in `TopologyManagerReadService` to `store` because it doesn't act anymore as a string filter like `filter_party`.
- **BREAKING CHANGE** Console commands changed the parameter `filterStore: String` to `store: TopologyStoreId`. Additionally, there
  are implicit conversions in `ConsoleEnvironment` to convert `SynchronizerId` to `TopologyStoreId` and variants thereof (`Option`, `Set`, ...).
  With these implicit conversions, whenever a `TopologyStoreId` is expected, users can pass just the synchronizer id and it will be automatically converted
  into the correct `TopologyStoreId.Synchronizer`.

- Reduced the payload size of an ACS commitment from 2kB to 34 bytes.

- **BREAKING CHANGE** Changed the endpoint `PackageService.UploadDar` to accept a list of dars that can be uploaded and vetted together.
  The same change is also represented in the `ParticipantAdminCommands.Package.UploadDar`.

## Until 2025-02-12 (Exclusive)
- Added the concept of temporary topology stores. A temporary topology store is not connected to any synchronizer store
  and therefore does not automatically submit transactions to synchronizers. Temporary topology stores can be used
  for the synchronizer bootstrapping ceremony to not "pollute" the synchronizer owners' authorized stores. Another use
  case is to upload a topology snapshot and inspect the snapshot via the usual topology read service endpoints.
  - Temporary topology stores can be managed via the services `TopologyManagerWriteService.CreateTemporaryTopologyStore` and `TopologyManagerWriteService.DropTemporaryTopologyStore`.
  - **BREAKING CHANGE**: The `string store` parameters in the `TopologyManagerWriteService` have been changed to `StoreId store`.

## Until 2025-01-29 (Exclusive)
- Added a buffer for serving events that is limited by an upper bound for memory consumption:
    ```hocon
        canton.sequencers.<sequencer>.sequencer.block.writer {
          type = high-throughput // NB: this is required for the writer config to be parsed properly

          // maximum memory the buffered events will occupy
          buffered-events-max-memory = 2MiB // Default value
          // batch size for warming up the events buffer at the start of the sequencer until the buffer is full
          buffered-events-preload-batch-size = 50 // Default value
        }
    ```
  - The previous setting `canton.sequencers.<sequencer>.sequencer.block.writer.max-buffered-events-size` has been removed and has no effect anymore
- The sequencer's payload cache configuration changed slightly to disambiguate the memory-limit config from a number-of-elements config:
    ```hocon
    canton.sequencers.<sequencer>.parameters.caching {
      sequencer-payload-cache {
        expire-after-access = "1 minute" // Default value
        maximum-memory = 200MiB // Default value
      }
    }
    ```
  - The previous setting `canton.sequencers.<sequencer>.parameters.caching.sequencer-payload-cache.maximum-size` has been removed and has no effect anymore.

## Until 2025-01-22 (Exclusive)
- Changed the console User.isActive to isDeactivated to align with the Ledger API
- Added new prototype for declarative api
- Added metric `daml.mediator.approved-requests.total` to count the number of approved confirmation requests
- Topology related error codes have been renamed to contain the prefix `TOPOLOGY_`:
  - Simple additions of prefix
    - `SECRET_KEY_NOT_IN_STORE` -> `TOPOLOGY_SECRET_KEY_NOT_IN_STORE`
    - `SERIAL_MISMATCH` -> `TOPOLOGY_SERIAL_MISMATCH`
    - `INVALID_SYNCHRONIZER` -> `TOPOLOGY_INVALID_SYNCHRONIZER`
    - `NO_APPROPRIATE_SIGNING_KEY_IN_STORE` -> `TOPOLOGY_NO_APPROPRIATE_SIGNING_KEY_IN_STORE`
    - `NO_CORRESPONDING_ACTIVE_TX_TO_REVOKE` -> `TOPOLOGY_NO_CORRESPONDING_ACTIVE_TX_TO_REVOKE`
    - `REMOVING_LAST_KEY_MUST_BE_FORCED` -> `TOPOLOGY_REMOVING_LAST_KEY_MUST_BE_FORCED`
    - `DANGEROUS_COMMAND_REQUIRES_FORCE_ALIEN_MEMBER` -> `TOPOLOGY_DANGEROUS_COMMAND_REQUIRES_FORCE_ALIEN_MEMBER`
    - `REMOVING_KEY_DANGLING_TRANSACTIONS_MUST_BE_FORCED` -> `TOPOLOGY_REMOVING_KEY_DANGLING_TRANSACTIONS_MUST_BE_FORCED`
    - `INCREASE_OF_SUBMISSION_TIME_TOLERANCE` -> `TOPOLOGY_INCREASE_OF_SUBMISSION_TIME_TOLERANCE`
    - `INSUFFICIENT_KEYS` -> `TOPOLOGY_INSUFFICIENT_KEYS`
    - `UNKNOWN_MEMBERS` -> `TOPOLOGY_UNKNOWN_MEMBERS`
    - `UNKNOWN_PARTIES` -> `TOPOLOGY_UNKNOWN_PARTIES`
    - `ILLEGAL_REMOVAL_OF_SYNCHRONIZER_TRUST_CERTIFICATE` -> `TOPOLOGY_ILLEGAL_REMOVAL_OF_SYNCHRONIZER_TRUST_CERTIFICATE`
    - `PARTICIPANT_ONBOARDING_REFUSED` -> `TOPOLOGY_PARTICIPANT_ONBOARDING_REFUSED`
    - `MEDIATORS_ALREADY_IN_OTHER_GROUPS` -> `TOPOLOGY_MEDIATORS_ALREADY_IN_OTHER_GROUPS`
    - `MEMBER_CANNOT_REJOIN_SYNCHRONIZER` -> `TOPOLOGY_MEMBER_CANNOT_REJOIN_SYNCHRONIZER`
    - `NAMESPACE_ALREADY_IN_USE` -> `TOPOLOGY_NAMESPACE_ALREADY_IN_USE`
    - `DANGEROUS_VETTING_COMMAND_REQUIRES_FORCE_FLAG` -> `TOPOLOGY_DANGEROUS_VETTING_COMMAND_REQUIRES_FORCE_FLAG`
    - `DEPENDENCIES_NOT_VETTED` -> `TOPOLOGY_DEPENDENCIES_NOT_VETTED`
    - `CANNOT_VET_DUE_TO_MISSING_PACKAGES` -> `TOPOLOGY_CANNOT_VET_DUE_TO_MISSING_PACKAGES`
  - Additional minor renaming
    - `INVALID_TOPOLOGY_TX_SIGNATURE_ERROR` -> `TOPOLOGY_INVALID_TOPOLOGY_TX_SIGNATURE`
    - `DUPLICATE_TOPOLOGY_TRANSACTION` -> `TOPOLOGY_DUPLICATE_TRANSACTION`
    - `UNAUTHORIZED_TOPOLOGY_TRANSACTION` -> `TOPOLOGY_UNAUTHORIZED_TRANSACTION`
    - `INVALID_TOPOLOGY_MAPPING` -> `TOPOLOGY_INVALID_MAPPING`
    - `INCONSISTENT_TOPOLOGY_SNAPSHOT` -> `TOPOLOGY_INCONSISTENT_SNAPSHOT`
    - `MISSING_TOPOLOGY_MAPPING` -> `TOPOLOGY_MISSING_MAPPING`
- Added the last_descendant_node_id field in the exercised event of the ledger api. This field specifies the upper
  boundary of the node ids of the events in the same transaction that appeared as a result of the exercised event.
- Removed the child_node_ids and the root_node_ids fields from the exercised event of the ledger api. After this change
  it will be possible to check that an event is child of another or a root event through the descendant relationship
  using the last_descendant_node_id field.

## Until 2025-01-15 (Exclusive)

- Renamed request/response protobuf messages of the inspection, pruning, resource management services from `Endpoint.Request` to `EndpointRequest` and respectively for the response types.
- Renamed the node_index field of events in the index db to node_id.
- Changes to defaults in ResourceLimits:
  - The fields `max_inflight_validation_requests` and `max_submission_rate` are now declared as `optional uint32`,
    which also means that absent values are not encoded anymore as negative values, but as absent values.
    Negative values will result in a parsing error and a rejected request.
- Moved the `canton.monitoring.log-query-cost` option to `canton.monitoring.logging.query-cost`
- Changed the `signedBy` parameter of the console command `topology.party_to_participant_mapping.propose` from `Optional`
  to `Seq`.

## Until 2025-01-10 (Exclusive)

### Initial Topology Snapshot Validation
The initial topology snapshot, both for initializing a new domain and for onboarding a new member,
is now validated by the node importing the snapshot.

In case the snapshot might contain legacy OTK topology transactions with missing signatures for newly added signing keys,
the nodes may permit such transactions by overriding the following setting:

```
canton.sequencers.mySequencer.topology.insecure-ignore-missing-extra-key-signatures-in-initial-snapshot = true

canton.participants.myParticipant.topology.insecure-ignore-missing-extra-key-signatures-in-initial-snapshot = true

canton.mediators.myMediator.topology.insecure-ignore-missing-extra-key-signatures-in-initial-snapshot = true
```

## Until 2025-01-04 (Exclusive)
- The event_id field has been removed from the Event messages of the lapi since now the event id consists of the offset
  and the node id which are already present in the events.
- The events_by_id field in the TransactionTree message has been converted from a map<string, TreeEvent> to a
  map<int32, TreeEvent> with values the node ids of the events.
- Accordingly, the root_event_ids has been renamed to root_node_ids to hold the node ids of the root events.

## Until 2025-01-03 (Exclusive)

- We introduced contract key prefetching / bulk loading to improve workloads that fetch many contract keys.
- Domain renaming
    - domain id -> synchronizer id
    - domain alias -> synchronizer alias
    - domain projects (e.g., community-domain) -> synchronizer projects

## Until 2024-12-20 (Exclusive)
- The GetTransactionByEventId and the GetTransactionTreeByEventId endpoints of the lapi update service have been
  replaced by the GetTransactionByOffset and the GetTransactionTreeByOffset respectively.
    - As a consequence, the GetTransactionByEventIdRequest has been replaced by the GetTransactionByOffsetRequest message.
    - The GetTransactionByOffsetRequest contains the offset of the transaction or the transaction tree to be fetched and
      the requesting parties.
    - The json endpoints have been adapted accordingly

## Until 2024-12-17 (Exclusive)

### Refactored domain connectivity service
Refactored domain connectivity service to have endpoints with limited responsibilities:

- Add: ReconnectDomain to be able to reconnect to a registered domain
- Add: DisconnectAllDomains to disconnect from all connected domains
- Change: RegisterDomain does not allow to fully connect to a domain anymore (only registration and potentially handshake): if you want to connect to a domain, use the other endpoint
- Change: ConnectDomain takes a domain config so that it can be used to connect to a domain for the first time
- Rename: ListConfiguredDomains to ListRegisteredDomains for consistency (and in general: configure(d) -> register(ed))

### Memory check during node startup
A memory check has been introduced when starting the node. This check compares the memory allocated to the container with the -Xmx JVM option.
The goal is to ensure that the container has sufficient memory to run the application.
To configure the memory check behavior, add one of the following to your configuration:

```
canton.parameters.startup-memory-check-config.reporting-level = warn  // Default behavior: Logs a warning.
canton.parameters.startup-memory-check-config.reporting-level = crash // Terminates the node if the check fails.
canton.parameters.startup-memory-check-config.reporting-level = ignore // Skips the memory check entirely.
```

## Until 2024-12-03 (Exclusive)

- Removed parameters `sequencer.writer.event-write-batch-max-duration` and `sequencer.writer.payload-write-batch-max-duration` as these are not used anymore.
- Introduced parameter `sequencer.writer.event-write-max-concurrency` (default: 2) to configure the maximum number of events batches that can be written at a time.
- [Breaking Change]: `TopologyManagerReadService.ExportTopologySnapshot` and `TopologyManagerWriteService.ImportTopologySnapshot` are now streaming services for exporting and importing a topology snapshot respectively.

## Until 2024-12-02 (Exclusive)

### Integer event ids in ledger api
- Added offset (int64) and node-id (int32) fields in all the event types in the ledger api.
  The following messages have the additional fields:
  - CreatedEvent
  - ArchivedEvent
  - ExercisedEvent
- Accordingly the java bindings and json schema were augmented to include the new fields.

## Until 2024-11-28 (Exclusive)
- Deduplication Offset extension to accept participant begin

  Before, only absolute offsets were allowed to define the deduplication periods by offset. After the change
  participant-begin offsets are also supported for defining deduplication periods. The participant-begin deduplication
  period (defined as zero value in API) is only valid to be used if the participant was not pruned yet. Otherwise, as in
  the other cases where the deduplication offset is earlier than the last pruned offset, an error informing that
  deduplication period starts too early will be returned.

## Until 2024-11-27 (Exclusive)
- Index DB schema changed in a non-backwards compatible fashion.

  The offset-related fields (e.g. ledger_offset, ledger_end) that were previously stored as `VARCHAR(4000)` for H2 and
    `text` for Postgres are now stored as `BIGINT` (for both db types).
  - If the offset column can take the value of the participant begin then the column should be null-able and null should
    be stored as the offset value (i.e. no zero values are used to represent the participant begin).
  - Only exception to
    it is the deduplication_offset of the lapi_command_completions which will take the zero value when the participant
    begin must be stored as deduplication offset, since null is used to signify the absence of this field.
- Changed DeduplicationPeriod's offset field type to `int64` in participant_transaction.proto in a non-backwards
  compatible fashion.

  The type of the offset field changed from `bytes` to `int64` to be compatible with the newly introduced intefer offset type.

## Until 2024-11-16 (Exclusive)

- [Breaking Change] renamed configuration parameter `session-key-cache-config` to `session-encryption-key-cache`.
- `sequencer_authentication_service` RPCs return failures as gRPC errors instead of a dedicated failure message with status OK.

## Until 2024-11-13 (Exclusive)
- display_name is no longer a part of Party data, so is removed from party allocation and update requests in the ledger api and daml script
- `PartyNameManagement` service was removed from the ledger api

## Until 2024-11-09 (Exclusive)

- When a Grpc channel is open or closed on the Ledger API, a message is logged at a debug level:
```
[..] DEBUG c.d.c.p.a.GrpcConnectionLogger:participant=participant - Grpc connection open: {io.grpc.Grpc.TRANSPORT_ATTR_LOCAL_ADDR=/127.0.0.1:5001, io.grpc.internal.GrpcAttributes.securityLevel=NONE, io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR=/127.0.0.1:49944}
[..] DEBUG c.d.c.p.a.GrpcConnectionLogger:participant=participant - Grpc connection closed: {io.grpc.Grpc.TRANSPORT_ATTR_LOCAL_ADDR=/127.0.0.1:5001, io.grpc.internal.GrpcAttributes.securityLevel=NONE, io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR=/127.0.0.1:49944}
```
- The keep alive behavior of the Ledger API can be configured through
```
canton.participants.participant.ledger-api.keep-alive-server.*
```
- The default values of the keep alive configuration for the ledger api has been set to
```
time: 10m
timeout: 20s
permitKeepAliveTime: 10s
permitKeepAliveWithoutCalls: false
```
- The effective settings are reported by the Participant Node at the initialization time with a logline:
```
2024-10-31 18:09:34,258 [canton-env-ec-35] INFO  c.d.c.p.a.LedgerApiService:participant=participant - Listening on localhost:5001 over plain text with LedgerApiKeepAliveServerConfig(10m,20s,10s,true).
```
- New parameter value for `permitKeepAliveWithoutCalls` has been introduced to all keep alive configurations.
When set, it allows the clients to send keep alive signals outside any ongoing grpc call.
- Identical implementations `EnterpriseCantonStatus` and `CommunityCantonStatus` have been merged into a single class `CantonStatus`.

- A participant will now crash in exceptional cases during transaction validation instead of remaining in a failed state

## Until 2024-10-31 (Exclusive)

- Addition of a `submissionTimeRecordTimeTolerance` dynamic domain parameter, which defaults to the value of `ledgerTimRecordTimeTolerance`
- `ledgerTimRecordTimeTolerance` is no longer unsafe to increase, however, `submissionTimeRecordTimeTolerance` now is, within the same restrictions as `ledgerTimRecordTimeTolerance` was before
- Use of the flag `LedgerTimeRecordTimeToleranceIncrease` is now deprecated
- A new flag `SubmissionTimeRecordTimeToleranceIncrease` has been added to forcefully increase the `submissionTimeRecordTimeTolerance` instead

## Until 2024-10-28 (Exclusive)

- Split the current signing schemes into a key `([Encryption/Signing]KeySpec)` and algorithm `([Encryption/Signing]AlgorithmSpec)` specifications.
  We also changed the way this is configured in Canton, for example, `signing.default = ec-dsa-p-256` is now represented as:
  `signing.algorithms.default = ec-dsa-sha-256` and `signing.keys.default = ec-p-256`. This is not a breaking change because the old schemes are still accepted.
- [Breaking Change] changed the `name` parameter of `rotate_node_key` from `Option` to `String`.
- Added a `name: String` parameter to `rotate_kms_node_key`, allowing operators to specify a name for the new key.

## Until 2024-10-23 (Exclusive)

- Console commands use now integer offsets. The affected commands are the following:
  - ledger_api.updates.{trees, trees_with_tx_filter, subscribe_trees}
  - ledger_api.updates.{flat, flat_with_tx_filter, subscribe_flat}
  - ledger_api.state.end
  - ledger_api.state.acs.{of_party, active_contracts_of_party, incomplete_unassigned_of_party, incomplete_assigned_of_party, of_all}
  - ledger_api.completions.{list, subscribe}
  - ledger_api.javaapi.updates.{trees, flat, flat_with_tx_filter}
  - pruning.{prune, find_safe_offset, get_offset_by_time, prune_internally}
  - testing.state_inspection.lookupPublicationTime
- In the canton's pruning and inspection services we used strings to represent the offset of a participant.
  The integer approach replaces string representation in:
  - pruning service:
    - PruneRequest message: with int64
    - GetSafePruningOffsetRequest message: with int64
    - GetSafePruningOffsetResponse message: with int64
  - inspection service:
    - LookupOffsetByTime.Response: with optional int64.
        - If specified, it must be a valid absolute offset (positive integer).
        - If not set, no offset corresponding to the timestamp given exists.


## Until 2024-10-23 (Exclusive)

- Index DB schema changed in a non-backwards compatible fashion.
- gRPC requests that are aborted due to shutdown server-side return `CANCELLED` instead of `FAILED_PRECONDITION`.
- Added auto vacuuming defaults for sequencer tables for Postgres (will be set using database schema migrations).
- Removed support for Postgres 11, 12
- Made Postgres 14 default in the CI
- Don't fetch payloads for events with `eventCounter < subscriptionStartCounter`.
- Payloads are fetched behind a Caffeine cache.
```hocon
canton.sequencers.<sequencer>.parameters.caching {
  sequencer-payload-cache {
    expire-after-access="1 minute" // default value
    maximum-size="1000" // default value
  }
}
```
- Payload fetching can be configured with the following config settings:
```hocon
canton.sequencers.<sequencer>.sequencer.block.reader {
  // max number of payloads to fetch from the datastore in one page
  payload-batch-size = 10 // default value
  // max time window to wait for more payloads before fetching the current batch from the datastore
  payload-batch-window = "5ms" // default value
  // how many batches of payloads will be fetched in parallel
  payload-fetch-parallelism = 2 // default value
  // how many events will be generated from the fetched payloads in parallel
  event-generation-parallelism = 4 // default value
}
```
- Added sequencer in-memory fan out. Sequencer now holds last configurable number of events it has processed in memory.
  In practice this is 1-5 seconds worth of data with the default max buffer size of 2000 events. If the read request for
  a member subscription is within the fan out range, the sequencer will serve the event directly from memory, not performing
  any database queries. This feature is enabled by default and can be configured with the following settings:
```hocon
canton.sequencers.<sequencer>.sequencer.writer {
  type = high-throughput // NB: this is required for the writer config to be parsed properly
  max-buffered-events-size = 2000 // Default value
}
```
This feature greatly improves scalability of sequencer in the number of concurrent subscription, under an assumption that
members are reading events in a timely manner. If the fan out range is exceeded, the sequencer will fall back to reading
from the database. Longer fan out range can be configured, trading off memory usage for database load reduction.

- CommandService.SubmitAndWaitForUpdateId becomes CommandService.SubmitAndWait in terms of semantics and request/response payloads. The legacy SubmitAndWait form that returns an Empty response is removed from the CommandService
- Improved logging in case of sequencer connectivity problems as requested by Canton Network.
- The block sequencer is now configurable under `canton.sequencers.<sequencer>.block`, including new checkpoint settings:
```hocon
// how often checkpoints should be written
block.writer.checkpoint-interval = "30s"

// how many checkpoints should be written when backfilling checkpoints at startup
block.writer.checkpoint-backfill-parallelism = 2
```

- `IdentityInitializationService.CurrentTimeResponse` returns the current time in microseconds since epoch instead of a Google protobuf timestamp.
- Commands.DisclosedContract is enriched with `domain_id` which specifies the ID of the domain where the contract is currently assigned.
  This field is currently optional to ensure backwards compatibility. When specified, the domain-id of the disclosed contracts that
  are used in command interpretation is used to route the command submission to the specified domain-id. In case of domain-id mismatches,
  the possible errors are reported as command rejections with the either `DISCLOSED_CONTRACTS_DOMAIN_ID_MISMATCH` or `PRESCRIBED_DOMAIN_ID_MISMATCH` self-service error codes.

## Until 2024-10-16 (Exclusive)

- New config option `parameters.timeouts.processing.sequenced-event-processing-bound` allows to specify a timeout for processing sequenced events. When processing takes longer on a node, the node will log an error or crash (depending on the `exit-on-fatal-failures` parameter).
- Fixed a crash recovery bug in unified sequencer, when it can miss events in the recovery process. Now it will start from
  the correct earlier block height in these situations.

## Until 2024-10-02 (Exclusive)

- Removed party-level group addressing.
- `parallel_indexer` metrics have been renamed to simply `indexer`, i.e.
```daml_participant_api_parallel_indexer_inputmapping_batch_size_bucket```
becomes
```daml_participant_api_indexer_inputmapping_batch_size_bucket```
- Completely removed leftovers in the code of Oracle support.

## Until 2024-09-26 (Exclusive)

- Pruning and scheduled pruning along with pruning configuration have moved from enterprise to community. One slight caveat is scheduled sequencer pruning which is currently only wired up in the enterprise database sequencer.

## Until 2024-09-20 (Exclusive)

- Sequencer types `type = external` and `type = BFT` can now configure the underlying block sequencer in the config section `canton.sequencers.<sequencer>.block` and uses the same `reader` and `writer` configuration as the `type = database` sequencer.

```hocon
canton {
  sequencers {
    sequencer1 {
      type = external
      config = {
        // config for external sequencer (eg CometBFT)
      }
      block {
        writer.checkpoint-interval = "10s"
        checkpoint-backfill-parallelism = 2
        reader.read-batch-size = 50
      }
    }
  }
}
```

## Until 2024-09-18 (Exclusive)

- Improve organization and layout of Ledger API Reference docs.

## Until 2024-09-17 (Exclusive)

### Integer Offset in ledger api
In the ledger api protobufs we used strings to represent the offset of a participant.
The integer approach replaces string representation in:
- OffsetCheckpoint message: with int64
- CompletionStreamRequest message of command completion service: with int64.
  - If specified, it must be a valid absolute offset (positive integer) or zero (ledger begin offset)..
  - If not set, the ledger uses the ledger begin offset instead.
- GetLedgerEndResponse message: with int64
  - It will always be a non-negative integer.
  - If zero, the participant view of the ledger is empty.
  - If positive, the absolute offset of the ledger as viewed by the participant.
- GetLatestPrunedOffsetsResponse message: with int64
  - If positive, it is a valid absolute offset (positive integer).
  - If zero, no pruning has happened yet.
- SubmitAndWaitForUpdateIdResponse message: with int64
- PruneRequest message (prune_up_to): with int64
- Reassignment, TransactionTree, Transaction and Completion (offset, deduplication_offset) message: with int64
- Commands message (deduplication_offset): with int64
- GetActiveContractsRequest message (active_at_offset): with int64 (non-negative offset expected)
  - If zero, the empty set will be returned
  - Note that previously if this field was not set the current ledger end was implicitly derived. This is no longer possible.
- GetActiveContractsResponse message: removed the offset field
- GetUpdatesRequest message,
  - begin_exclusive: with int64 (non-negative offset expected)
  - end_inclusive: with optional int64
    - If specified, it must be a valid absolute offset (positive integer).
    - If not set, the stream will not terminate.

## Until 2024-09-16 (Exclusive)

- Re-onboarding members results in a rejection of the `DomainTrustCertificate`, `SequencerDomainState`, or `MediatorDomainState` with the error `MEMBER_CANNOT_REJOIN_DOMAIN`.

## Until 2024-09-06 (Exclusive)

- Console.bootstrap.domain has new parameter domainThreshold, the minimum number of domain owners that need to authorize on behalf of the domain's namespace.
- [Breaking change]: added a new mandatory `usage: SigningKeyUsage` parameter for the `register_kms_signing_key()` and the `generate_signing_key()` commands. This new parameter is used to specify the type of usage the new key will have.
  It can take the following usage types:
    - `Namespace`: the root namespace key that defines a node's identity and signs topology requests;
    - `IdentityDelegation`: a signing key that acts as a delegation key for the root namespace and that can also be used to sign topology requests;
    - `SequencerAuthentication`: a signing key that authenticates members of the network towards a sequencer;
    - `Protocol`: a signing key that deals with all the signing that happens as part of the protocol.
  This separation makes our system more robust in case of a compromised key.

## Until 2024-09-04 (Exclusive)

- google.protobuf.XValue wrapper messages are replaced by `optional X` in the protobuf definitions. Incompatibility for manually crafted Protobuf messages and wire formats. Protobuf bindings must be regenerated, but should remain compatible.
- Started the renaming transfer -> reassignment
  - transferExclusivityTimeout -> assignmentExclusivityTimeout
- Added periodic generation of sequencer counter checkpoints to the sequencer and reworked SQL queries.
  - This should improve performance for sequencer snapshotting and pruning and reduce database load.
  - The checkpoint interval is configurable under `canton.sequencers.<sequencer>.writer.checkpoint-interval` (default: 30s):
```hocon
writer {
  checkpoint-interval = "30s"
}
```

## Until 2024-08-30 (Exclusive)
- The `ParticipantOffset` message was removed since it was already replaced by a simpler string representation and
  was not used anymore.

## Until 2024-08-28 (Exclusive)
- the DomainId field has been removed from the following topology mapping: `OwnerToKeyMapping`, `VettedPackages`, `PartyToParticipant` and `AuthorityOf`.
  Those fields were not handled properly, so we decide to remove them.
- two new endpoints added to `GrpcInspectionService` to inspect the state of sent and received ACS commitments on participants.
  - `lookupSentAcsCommitments` to retrieve sent ACS Commitments and their states
  - `lookupReceivedAcsCommitments` to retrieve received ACS commitments and their states
- When not specifying `AuthorizeRequest.signed_by` or `SignTransactionsRequest.signed_by`, suitable signing keys available to the node are selected automatically.

## Until 2024-08-26 (Exclusive)

### Changes in `VersionService.GetLedgerApiVersion`
- The `GetLedgerApiVersion` method of the `VersionService` contains new `features.offset_checkpoint` field within the returned `GetLedgerApiVersionResponse` message.
  It exposes the `max_offset_checkpoint_emission_delay` which is the maximum time needed to emit a new OffsetCheckpoint.

## Until 2024-08-21 (Exclusive)
- Error INVALID_SUBMITTER is changed to INVALID_READER
- Config of the jwt token leeway has been moved from `participants.participant.parameters.ledger-api-server.jwt-timestamp-leeway` to `participants.participant.ledger-api.jwt-timestamp-leeway`
- Creating a `MediatorDomainState` fails if a mediator is both in the `active` and the `observers` lists.
- Creating a `SequencerDomainState` fails if a sequencer is both in the `active` and the `observers` lists.

### New `logout()` commands
In case it is suspected that a member's authentication tokens for the public sequencer API have been leaked or somehow compromised,
we introduced new administration commands that allow an operator to revoke all the authentication tokens for a member and close the sequencer connections.
The legitimate member then automatically reconnects and obtains new tokens.
The commands are accessible via the console as, for example:
- `participant1.domains.logout(myDomainAlias)`
- `mediator1.sequencer_connections.logout()`

### Package vetting validation
We have introduced additional package vetting validations that may result in package rejections:
- You cannot unvet a package unless you provide the force flag: FORCE_FLAG_ALLOW_UNVET_PACKAGE.
- You cannot vet a package that has not yet been uploaded unless you provide the force flag: FORCE_FLAG_ALLOW_UNKNOWN_PACKAGE.
- You cannot vet a package if its dependencies have not yet been vetted, unless you provide the force flag: FORCE_FLAG_ALLOW_UNVETTED_DEPENDENCIES.

### Mediators may not be in two mediator groups at the same time
Add mediators to multiple groups results in a rejection with error `MEDIATORS_ALREADY_IN_OTHER_GROUPS`.

### Traffic purchase handler returns early
SetTrafficPurchased requests return immediately and no longer return the max sequencing time.

## Until 2024-07-31 (Exclusive)

- Removed the GrpcTransferService
- Renamed metric `daml_sequencer_client_handler_delay` => `daml_block_delay` (sequencer block processing delay relative to sequencers local wall clock)
- Added new metric `daml_sequencer_db_watermark_delay` (database sequencer watermark delay relative to sequencers local wall clock)

### OffsetCheckpoint in completions stream

To support OffsetCheckpoints in completions stream changes are made to command completion service protobuf definitions.
- The Checkpoint message and the domain_id have been deleted from CompletionStreamResponse message. The domain id, offset
  and record time are now encapsulated in Completion in the following way:
  - an additional offset field to hold the offset
  - an additional domain_time field to hold the (domain_id, record_time) pair
- The CompletionStreamResponse has been converted to oneof Completion and OffsetCheckpoint in the following way:
  ```protobuf
  message CompletionStreamResponse {
    Checkpoint checkpoint = 1;
    Completion completion = 2;
    string domain_id = 3;
  }
  ```
  to
  ```protobuf
  message CompletionStreamResponse {
    oneof completion_response {
      Completion completion = 1;
      OffsetCheckpoint offset_checkpoint = 2;
    }
  }
  ```


## Until 2024-07-24 (Exclusive)

## Until 2024-07-17 (Exclusive)

- The `jwt-rs-256-jwks` auth service type in the `participant.ledger-api.auth-services` configuration has been changed to `jwt-jwks` to better represent the generic nature of the JWKS authorization.

### Consolidated ledger api changes up to date:
  - Additive change: new ``CommandInspectionService``
    - CommandInspectionService added to ``v2/admin``
    - Change in ``VersionService.GetVersion``, the response extended with ``ExperimentalCommandInspectionService`` signalling presence of the new service
  - Additive change: ``PackageManagementService`` extended with new method ``ValidateDarFile``
  - Additive change: Paging added to ``ListKnownParties`` of the ``PartyManagementService``
    - New fields in ``ListKnownPartiesRequest``
    - New fields in ``ListKnownPartiesResponse``
    - Change in ``VersionService.GetVersion``, the response extended with ``PartyManagementFeature`` signalling paging support and max page size
  - Additive change: User management rights extended with a new claim ``CanReadAsAnyParty``
  - Additive change: Party wildcard supported in ``TransactionFilter`` through ``filters_for_any_party``
  - <span style="color:red">Breaking change</span>: Complete rewrite of the filtering in the ``TransactionFilter``
    - Filters message changed ``InclusiveFilters inclusive`` becomes ``repeated CumulativeFilter cumulative``
    - ``InclusiveFilters`` message removed in favor of ``CumulativeFilter``
    - ``WildcardFilter`` message added
    - ``Filters`` message cannot be empty

## Until 2024-07-15 (Exclusive)

The following changes are not included into release-line-3.1.

### Simplified Offset in ledger api
In the ledger api protobufs we used both ParticipantOffset message and strings to represent the offset of a participant.
The simpler string approach replaces ParticipantOffset in:
- GetLedgerEndResponse message, where an empty string denotes the participant begin offset
- GetLatestPrunedOffsetsResponse message, where an empty string denotes that participant is not pruned so far
- GetUpdatesRequest message,
    - begin_exclusive is now a string where previous participant-offset values are mapped in the following manner:
        - `ParticipantOffset.ParticipantBoundary.PARTICIPANT_BOUNDARY_BEGIN` is represented by an empty string
        - `ParticipantOffset.Absolute` is represented by a populated string
        - `ParticipantOffset.ParticipantBoundary.PARTICIPANT_BOUNDARY_END`  cannot be represented anymore and previous
          references should be replaced by a prior call to retrieve the ledger end
        - absence of a value was invalid
    - end_inclusive is now a string where previous participant-offset values are mapped in the following manner:
        - `ParticipantOffset.ParticipantBoundary.PARTICIPANT_BOUNDARY_BEGIN` cannot be represented anymore
        - `ParticipantOffset.Absolute` is represented by a populated string
        - `ParticipantOffset.ParticipantBoundary.PARTICIPANT_BOUNDARY_END`  cannot be represented anymore and previous
          references should be replaced by a prior call to retrieve the ledger end
        - absence of a value signifying an open-ended tailing stream is represented by an empty string

## Until 2024-07-10 (Exclusive)
- The endpoint to download the genesis state for the sequencer is now available on all nodes, and it has been removed from the sequencer admin commands.
  - To download the genesis state use: `sequencer1.topology.transactions.genesis_state()` instead of `sequencer.setup.genesis_state_for_sequencer()`
- A config option to randomize token life `canton.sequencers.<sequencer>.public-api.use-exponential-random-token-expiration = true|false` (defaults to `false`).
  When enabled, it samples token life duration from an exponential distribution with scale of `maxTokenExpirationInterval`,
  with the values truncated (re-sampled) to fit into an interval `[maxTokenExpirationInterval / 2, maxTokenExpirationInterval]`,
  so the token will be between half and the value specified in `maxTokenExpirationInterval`.
- Config option renamed to prevent confusion:
  - `canton.sequencers.<sequencer>.public-api.token-expiration-time` => `canton.sequencers.<sequencer>.public-api.max-token-expiration-interval`
  - `canton.sequencers.<sequencer>.public-api.nonce-expiration-time` => `canton.sequencers.<sequencer>.public-api.nonce-expiration-interval`
- Submission request amplification delays resending the submission request for a configurable patience. The sequencer connections' parameter `submission_request_amplification` is now a structured message of the previous factor and the patience.
- Paging in Party Management
    - The `ListKnownParties` method on the `PartyManagementService` now takes two additional parameters. The new `page_size` field determines the maximum number of results to be returned by the server. The new `page_token` field on the other hand is a continuation token that signals to the server to fetch the next page containing the results. Each `ListKnownPartiesResponse` response contains a page of parties and a `next_page_token` field that can be used to populate the `page_token` field for a subsequent request. When the last page is reached, the `next_page_token` is empty. The parties on each page are sorted in ascending order according to their ids. The pages themselves are sorted as well.
    - The `GetLedgerApiVersion` method of the `VersionService` contains new `features.party_management` field within the returned `GetLedgerApiVersionResponse` message. It describes the capabilities of the party management through a sub-message called `PartyManagementFeature`. At the moment it contains just one field the `max_parties_page_size` which specifies the maximum number of parties that will be sent per page by default.
    - The default maximum size of the page returned by the participant in response to the `ListKnownParties` call has been set to **10'0000**. It can be modified through the `max-parties-page-size` entry: <br/>
    ` canton.participants.participant.ledger-api.party-management-service.max-parties-page-size=777 `
- Mediator initialization cleanup
  - Removed `InitializeMediatorRequest.domain_parameters`
  - Removed `MediatorDomainConfiguration.initialKeyFingerprint` and corresponding entry in the database
  - The static parameters are determined from the set of sequencers provided during initialization via `mediator.setup.assign(...)` or the grpc admin api call `MediatorInitializationService.InitializeMediator`.
- Canton Node initialization cleanup
  - Renamed to remove `X` from `com.digitalasset.canton.topology.admin.v30.IdentityInitializationXService`
- Daml Logging works again, logging by default during phase 1 at Debug log level.
- The `NO_INTERNAL_PARTICIPANT_DATA_BEFORE` error code is introduced and returned when `participant.pruning.find_safe_offset` is invoked with a timestamp before the earliest
  known internal participant data. Before this change `find_safe_offset` used to return `None` in this case thus making it impossible to distinguish the situation
  from no safe offset existing. When `find_safe_offset` returns `NO_INTERNAL_PARTICIPANT_DATA_BEFORE`, it is safe to invoke `participant.pruning.prune` with
  an offset corresponding to the timestamp passed to `find_safe_offset`.
- `vetted_packages.propose_delta` no longer allows specifying a `serial` parameter, and instead increments the serial relative to the last authorized topology transaction.
- The new repair method `participant.repair.purge_deactivated_domain` allows removing data from the deactivated domain
  after a hard domain migration.
- Repair method `participant.repair.migrate_domain` features a new `force` flag. When set `true` it forces a domain
  migration ignoring in-flight transactions.
- Removed the protobuf message field `BaseQuery.filterOperation`. Setting the field `BaseQuery.operation` will use it as filter criteria.
- Sequencer subscription now will not return `InvalidCounter(...)` when sequencer cannot sign the event, now it will always return a tombstone with a `TombstoneEncountered` error.
This can happen when a newly onboarded sequencer cannot sign a submission originated before it was bootstrapped or if manually initialized sequencer cannot find its keys.
- When connecting to sequencer nodes, participants and mediators return once `sequencerTrustThreshold * 2 + 1` sequencers return valid endpoints unless `SequencerConnectionValidation.ALL` is requested.

### Simplified Offset in ledger api
In the ledger api protobufs we used both ParticipantOffset message and strings to represent the offset of a participant.
The simpler string approach replaces ParticipantOffset in:
 - Checkpoint message
 - CompletionStreamRequest of command completion service. In particular, the `begin_exclusive` field have been converted to string.
   Before, the absence of this field was denoting the participant end, while currently the empty string means the participant begin.
   Thus, if the completion stream starting from the participant end is needed the begin_exclusive offset has to be explicitly given
   by first querying for the participant end.

### Rework of the member IDs in protobuf
In the protobufs, we use `participant_id` to sometimes contain `PAR::uid` and sometimes only `uid`, without
the three-letter code and similar for the other member IDs. Moreover, `mediator` contains sometimes a uid
and sometimes the mediator group. The goal is to make it explicit what the field contains:

- Use _uid suffix if the field does not contain the three-letters code
- Use member if it can be any member (with the three-letters code)

Changed field:
SequencerConnect.GetDomainIdResponse.sequencer_id -> sequencer_uid (format changed, code removed)
SequencerNodeStatus.connected_participants -> connected_participant_uids (format changed, code removed)
OrderingRequest.sequencer_id -> OrderingRequest.sequencer_uid (format changed, code removed)
ListPartiesResponse.Result.ParticipantDomains.participant -> participant_uid (format changed, code removed)
OnboardingStateRequest.sequencer_id -> sequencer_uid (format changed, code removed)


### Package management backend unification
The Ledger API and Admin API gRPC services used for package management now use the same backend logic and storage. There is no Ledger/Admin API client impact,
but the following changes are breaking compatibility:
- `par_daml_packages` is extended with `package_size` and `uploaded_at`, both non-null. A fresh re-upload of all packages is required to conform.
- `ledger_sync_event.proto` drops the package notification ledger events: `public_package_upload` and `public_package_upload_rejected`
- `canton.participants.participant.parameters.ledger-api-server.indexer.package-metadata-view` has been moved to `canton.participants.participant.parameters.package-metadata-view`.
- `com.digitalasset.canton.admin.participant.v30.PackageService` `RemoveDar` and `RemovePackage` operations become dangerous and are not recommended for production usage anymore. Unadvised usage can lead to broken Ledger API if packages are removed for non-pruned events referencing them.
Additionally, as relevant but non-impacting changes:
- Ledger API Index database drops all references to package data. The Ledger API uses `par_daml_packages` or `par_dars` for all package/DARs operations.

### Alpha: Failed Command Inspection
In order to improve debugging of failed commands, the participant now stores the last few commands
(successes, failures and pending) in memory for debug inspection. The data is accessible through the
command inspection service on the ledger api.

### Split encryption scheme into algorithm and key scheme
Before we combined keys and crypto algorithms into a single key scheme, for example EciesP256HkdfHmacSha256Aes128Gcm and EciesP256HmacSha256Aes128Cbc.
The underlying EC key is on the P-256 curve and could be used with both AES-128-GCM and -CBC as part of a hybrid encryption scheme.
Therefore, we decided to split this scheme into a key `(EncryptionKeySpec)` and algorithm `(EncryptionAlgorithmSpec)` specifications.
We also changed the way this is configured in Canton, for example:
- `encryption.default = rsa-2048-oaep-sha-256` is now represented as:

   `encryption.algorithms.default = rsa-oaep-sha-256`
   `encryption.keys.default = rsa-2048`

### Bug Fixes

#### (24-022, Moderate): Participant replica does not clear package service cache

##### Issue Description

When a participant replica becomes active, it does not refresh the package dependency cache. If a vetting attempt is made on the participant that fails because the package is not uploaded, the "missing package" response is cached. If the package is then uploaded to another replica, and we switch to the original participant, this package service cache will still record the package as nonexistent. When the package is used in a transaction, we will get a local model conformance error as the transaction validator cannot find the package, whereas other parts of the participant that don't use the package service can successfully locate it.

##### Affected Deployments

Participant

##### Affected Versions
3.0, 3.1

##### Impact

Replica crashes during transaction validation.

##### Symptom

Validating participant emits warning:
```

LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK(5,a2b60642): Rejected transaction due to a failed model conformance check: UnvettedPackages
```
And then emits an error:
```
An internal error has occurred.
java.lang.IllegalStateException: Mediator approved a request that we have locally rejected
```

##### Workaround

Restart recently active replica

##### Likeliness

Likely to happen in any replicated participant setup with frequent vetting attempts and switches between active and passive replicated participants between those vetting attempts.

##### Recommendation

Users are advised to upgrade to the next minor release (3.2) during their maintenance window.


#### (24-015, Minor): Pointwise flat transaction Ledger API queries can unexpectedly return TRANSACTION_NOT_FOUND

##### Description
When a party submits a command that has no events for contracts whose stakeholders are amongst the submitters, the resulted transaction cannot be queried by pointwise flat transaction Ledger API queries. This impacts GetTransactionById, GetTransactionByEventId and SubmitAndWaitForTransaction gRPC endpoints.

##### Affected Deployments
Participant

##### Impact
User might perceive that a command was not successful even if it was.

##### Symptom
TRANSACTION_NOT_FOUND is returned on a query that is expected to succeed.

##### Workaround
Query instead the transaction tree by transaction-id to get the transaction details.

##### Likeliness
Lower likelihood as commands usually have events whose contracts' stakeholders are amongst the submitting parties.

##### Recommendation
Users are advised to upgrade to the next patch release during their maintenance window.

#### (24-010, Critical): Malformed requests can stay in an uncleaned state

##### Description
When a participant handles a malformed request (for instance because topology changed during the request processing and a party was added, causing the recipient list to be invalid), it will attempt to send a response to the mediator. If the sending fails (for instance because max sequencing time has elapsed), the request never gets cleaned up. This is not fixed by crash recovery because the same thing will happen again as max sequencing time is still elapsed, and therefore the request stays dirty.

##### Affected Deployments
Participant

##### Impact
An affected participant cannot be pruned above the last dirty request and crash recovery will take longer as it restarts from that request as well.

##### Symptom
The number of dirty requests reported by the participant never reaches 0.

##### Workaround
No workaround exists. You need to upgrade to a version not affected by this issue.

##### Likeliness
Not very likely as only triggered by specific malformed events followed by a failure to send the response the sequencer.
Concurrent topology changes and participant lagging behind the domain increase the odds of it happening.

##### Recommendation
Upgrade during your next maintenance window to a patch version not affected by this issue.

## Until 2024-06-05

- TransactionFilters have been extended to hold filters for party-wildcards:
### TransactionFilters proto
TransactionFilter message changed from
```protobuf
message TransactionFilter {
    map<string, Filters> filters_by_party = 1;
}
```
to
```protobuf
message TransactionFilter {
    map<string, Filters> filters_by_party = 1;

    Filters filters_for_any_party = 2;
}
```

- Filters changed to include a list of cumulative filters:
### Filters proto
Filters message changed from
```protobuf
message Filters {
    // Optional
    InclusiveFilters inclusive = 1;
}
```
to
```protobuf
message Filters {
    // Optional
    repeated CumulativeFilter cumulative = 1;
}
```

- Inclusive filters where changed to cumulative filter which support a Wildcard filter that matches all the templates (template-wildcard). Every filter in the cumulative list expands the scope of the resulting stream. Each interface, template or wildcard filter means additional events that will match the query.
### CumulativeFilter proto
InclusiveFilters message changed from
```protobuf
message InclusiveFilters {
  // Optional
  repeated InterfaceFilter interface_filters = 1;

  // Optional
  repeated TemplateFilter template_filters = 2;
}
```
to
```protobuf
message CumulativeFilter {
    oneof identifier_filter {
        // Optional
        WildcardFilter wildcard_filter = 1;

        // Optional
        InterfaceFilter interface_filter = 2;

        // Optional
        TemplateFilter template_filter = 3;
    }
}
```

- The new wildcard filter that is used to match all the templates (template-wildcard) includes the `include_created_event_blob` flag to control the presence of the `created_event_blob` in the returned `CreatedEvent`.
### WildcardFilter proto
WildcardFilter message added:
```protobuf
message WildcardFilter {
    // Optional
    bool include_created_event_blob = 1;
}
```

## Until 2024-05-16
- We changed the retry policy for checking the creation of KMS crypto keys to use exponential backoff, so the configuration for the `retry-config.create-key-check` is now done similarly as the `retry-config.failures`
    ```
    canton.participants.participant1.crypto.kms.retries.create-key-check {
          initial-delay = "0.1s",
          max-delay = "10 seconds",
          max-retries = 20,
    }
    ```

## Until 2024-03-20

- `health.running` is renamed to `health.is_running`
- `AcsCommitmentsCatchUpConfig` is removed from `StaticDomainParameters` in proto files
- When an access token expires and ledger api stream is terminated an `ABORTED(ACCESS_TOKEN_EXPIRED)` error is returned instead of `UNAUTHENTICATED(ACCESS_TOKEN_EXPIRED)`.

- The participant.domains.connect* methods have been modified in order to accommodate a new sequencer connection validation
  argument, which caused the existing commands to no longer work due to ambiguous default arguments. The connect methods
  will likely be reworked in the future to improve consistency and usability, as right now, there are too many of them with
  different capabilities and user experience.
- The `MetricsConfig` has been altered. The boolean argument `report-jvm-metrics` has been replaced with a more finegrained
  control over the available jvm metrics. Use `jvm-metrics.enabled = true` to recover the previous metrics.
- Many metrics have been renamed and restructured. In particular, labelled metrics are used now instead of
  the older ones where the node name was included in the metric name.
- The Jaeger trace exporter is no longer supported, as OpenTelemetry and Jaeger suggest to configure Jaeger
  using the otlp exporter instead of the custom Jaeger exporter.
- The arguments of the RateLimitConfig have been renamed, changing `maxDirtyRequests` to `maxInflightValidationRequests` and
  `maxRate` to `maxSubmissionRate` and `maxBurstFactor` to `maxSubmissionBurstFactor`.

### Topology operation proto
Operation changed from
```protobuf
enum TopologyChangeOp {
    // Adds a new or replaces an existing mapping
    TOPOLOGY_CHANGE_OP_REPLACE_UNSPECIFIED = 0;
    // Remove an existing mapping
    TOPOLOGY_CHANGE_OP_REMOVE = 1;
}
```
to
```protobuf
enum TopologyChangeOp {
    TOPOLOGY_CHANGE_OP_UNSPECIFIED = 0;

    // Adds a new or replaces an existing mapping
    TOPOLOGY_CHANGE_OP_ADD_REPLACE = 1;

    // Remove an existing mapping
    TOPOLOGY_CHANGE_OP_REMOVE = 2;
}
```
- `SequencerDriver.adminServices` now returns `Seq[ServerServiceDefinition]`

### Sequencer Initialization

The admin api for sequencer initialization has changed:

- `SequencerInitializationService.InitializeSequencer` is now called `SequencerInitializationService.InitializeSequencerFromGenesisState`. The `topology_snapshot` field is a versioned serialization of `StoredTopologyTransactionsX` (scala) / `TopologyTransactions` (protobuf).

- Onboarding a sequencer on an existing domain is now expected to work as follows:
  1. A node (usually one of the domain owners) uploads the new sequencer's identity transactions to the domain
  2. The domain owners add the sequencer to the SequencerDomainState
  3. A domain owner downloads the onboarding state via `SequencerAdministrationService.OnboardingState` and provides the returned opaque `bytes onboarding_state` to the new sequencer.
  4. The new sequencer then gets initialized with the opaque onboarding state via `SequencerInitializationService.InitializeSequencerFromOnboardingState`.

## Until 2024-03-13

- The default mediator admin api port has been changed to `6002`.
- Database sequencer writer and reader high throughput / high availability configuration defaults have been updated to optimize latency.

## Until 2024-03-06

- Ledger API field `Commands.workflow_id` at command submission cannot be used anymore for specifying the prescribed domain. For this purpose the usage of `Commands.domain_id` is available.

## Until 2024-02-21

- `SequencerConnections` now requires a `submissionRequestAmplification` field. By default, it should be set to 1.
- A few classes and configs were renamed:
  - Config `canton.mediators.mediator.caching.finalized-mediator-requests` -> `canton.mediators.mediator.caching.finalized-mediator-confirmation-requests`
  - DB column `response_aggregations.mediator_request` -> `response_aggregations.mediator_confirmation_request`
  - Proto: `com.digitalasset.canton.protocol.v30.MediatorResponse` -> `com.digitalasset.canton.protocol.v30.ConfirmationResponse`
  - Proto file renamed: `mediator_response.proto` -> `confirmation_response.proto`
  - Proto: `com.digitalasset.canton.protocol.v30.MalformedMediatorRequestResult` -> `com.digitalasset.canton.protocol.v30.MalformedMediatorConfirmationRequestResult`
  - Proto: `com.digitalasset.canton.protocol.v30.TypedSignedProtocolMessageContent` field: `mediator_response` -> `confirmation_response`
  - Proto: `com.digitalasset.canton.protocol.v30.TypedSignedProtocolMessageContent` field: `malformed_mediator_request_result` -> `malformed_mediator_confirmation_request_result`
  - Dynamic domain parameter and respective proto field: `com.digitalasset.canton.protocol.v30.DynamicDomainParameters.participant_response_timeout` -> `com.digitalasset.canton.protocol.v30.DynamicDomainParameters.confirmation_response_timeout`
  - Dynamic domain parameter: `maxRatePerParticipant` -> `confirmationRequestsMaxRate` and in its respective proto `com.digitalasset.canton.protocol.v30.ParticipantDomainLimits` field `max_rate` -> `confirmation_requests_max_rate`
- Removed support for optimistic validation of sequenced events (config option `optimistic-sequenced-event-validation` in the sequencer client config).

### Party replication
Console commands that allow to download an ACS snapshot now take a new mandatory argument to indicate whether
the snapshot will be used in the context of a party offboarding (party replication or not). This allows Canton to
performance additional checks and makes party offboarding safer.

Affected console command:
- `participant.repair.export_acs`

New argument: `partiesOffboarding: Boolean`.

### Topology Changes

- The scala type `ParticipantPermissionX` has been renamed to `ParticipantPermission` to reflect the changes in the proto files.

## Until 2024-02-12

- The GRPC proto files no longer contain the "X-nodes" or "topology-X" suffixes.
  Specifically the following changes require adaptation:

    - Topology mappings X-suffix removals with pattern `TopologyMappingX` -> `TopologyMapping`:
        - `NamespaceDelegation`, `IdentifierDelegation`, `OwnerToKeyMapping`, `TrafficControlState`, `VettedPackages`,
          `DecentralizedNamespaceDefinition`, `DomainTrustCertificate`, `ParticipantDomainPermission`, `PartyHostingLimits`,
          `PartyToParticipant`, `AuthorityOf`, `MediatorDomainState`, `SequencerDomainState`, `PurgeTopologyTransaction`, `DomainParametersState`
    - Services X removals: *XService -> *Service, *XRequest -> *Request, *XResponse -> *Response, specifically:
        - `TopologyManagerWriteService`, `TopologyManagerReadService`
    - Miscellaneous messages whose X-suffix has been removed
        - `StaticDomainParameters`, `TopologyTransactionsBroadcast`
        - `EnumsX` -> `Enums`
        - `EnumsX.TopologyChangeOpX` -> `Enums.TopologyChangeOp`
        - `EnumsX.ParticipantPermissionX` -> `Enums.ParticipantPermission`: In addition the following previous had an embedded _X_:
          `PARTICIPANT_PERMISSION_SUBMISSION`, `PARTICIPANT_PERMISSION_CONFIRMATION`, `PARTICIPANT_PERMISSION_OBSERVATION`, `PARTICIPANT_PERMISSION_UNSPECIFIED`

- Less importantly the old topology GRPC proto removals should not require adaptation. Note that some removals (marked `*` below)
  "make room" for the X-variants above to use the name, e.g. `NamespaceDelegation` formerly referring to the old "NSD"
  mapping, is now used for the daml 3.x-variant:

    - `TopologyChangeOp`*, `TrustLevel`, `ParticipantState`, `RequestSide`
    - Old topology mappings: `PartyToParticipant`*, `MediatorDomainState`, `NamespaceDelegation`*, `IdentifierDelegation`*,
      `OwnerToKeyMapping`*, `SignedLegalIdentityClaim`, `LegalIdentityClaim`, `VettedPackages`*,
      `TopologyStateUpdate`, `DomainParametersChange`
    - Old topology transactions: `SignedTopologyTransaction`*, `TopologyTransaction`*
    - Old topology services and messages: `TopologyManagerWriteService`*, `TopologyManagerReadService`*, `RegisterTopologyTransactionRequest`, `RegisterTopologyTransactionResponse`,
      `DomainTopologyTransactionMessage`

## Until 2024-02-08

- Renamed the following error codes:
  SEQUENCER_SIGNING_TIMESTAMP_TOO_EARLY to SEQUENCER_TOPOLOGY_TIMESTAMP_TOO_EARLY
  SEQUENCER_SIGNING_TIMESTAMP_AFTER_SEQUENCING_TIMESTAMP to SEQUENCER_TOPOLOGY_TIMESTAMP_AFTER_SEQUENCING_TIMESTAMP
  SEQUENCER_SIGNING_TIMESTAMP_MISSING to SEQUENCER_TOPOLOGY_TIMESTAMP_MISSING

## Until 2024-02-07

- Check that packages are valid upgrades of the package they claim to upgrade at upload-time in `ApiPackageManagementService`.

## Until 2024-02-06
- Executor Service Metrics removed
    The metrics for the execution services have been removed:

  - daml.executor.runtime.completed*
  - daml.executor.runtime.duration*
  - daml.executor.runtime.idle*
  - daml.executor.runtime.running*
  - daml.executor.runtime.submitted*
  - daml_executor_pool_size
  - daml_executor_pool_core
  - daml_executor_pool_max
  - daml_executor_pool_largest
  - daml_executor_threads_active
  - daml_executor_threads_running
  - daml_executor_tasks_queued
  - daml_executor_tasks_executing_queued
  - daml_executor_tasks_stolen
  - daml_executor_tasks_submitted
  - daml_executor_tasks_completed
  - daml_executor_tasks_queue_remaining

- The recipe for sequencer onboarding has changed to fetch the sequencer snapshot before the topology snapshot.
  The topology snapshot transactions should be filtered by the last (sequenced) timestamp ("lastTs") of the sequencer snapshot.

## Until 2024-02-03

- The `TrustLevel` was removed from the `ParticipantDomainPermissionX` proto and the fields were renumbered (see [#16887](https://github.com/DACH-NY/canton/pull/16887/files?w=1#diff-d2ee5cf3ffef141dd6f432d43a346d8fdb03c266227825fc56bbdbb4b0a826e6))

## Until 2024-01-26

- The `DomainAlias` in `*connect_local` is now non-optional
  - (i.e `participant.connect_local(sequencer, alias=Some(domainName))` is now `participant.connect_local(sequencer, alias=domainName)`)
- Participants cannot submit on behalf of parties with confirmation threshold > 1, even if they have submission permission.
- When an access token expires and stream is terminated an UNAUTHENTICATED(ACCESS_TOKEN_EXPIRED) error is returned.

## Until 2024-01-19

- Support for Unique Contract Key (UCK) semantics has been removed.
- The administration services have been restructured as follows:
  - `EnterpriseMediatorAdministrationService` is now `MediatorAdministrationService`.
  - `Snapshot` and `DisableMember` have been moved from `EnterpriseSequencerAdministrationService` to `SequencerAdministrationService`.
  - `EnterpriseSequencerAdministrationService` is now `SequencerPruningAdministrationService`.
  - `EnterpriseSequencerConnectionService` is now `SequencerConnectionService`.
  - The `AuthorizeLedgerIdentity` endpoint has been removed.
- `token-expiry-grace-period-for-streams` config parameter added.
- As part of daml 2.x, non-x-node removal:
  - Canton configuration now refers to nodes as "canton.participants", "canton.sequencers", and "canton.mediators"
    (rather than as "canton.participants-x", "canton.sequencers-x", and "canton.mediators-x").
  - Similarly remote nodes now reside under "canton.remote-participants", "canton.remote-sequencers", and
    "canton.remote-mediators" (i.e. the "-x" suffix has been removed).

## Until 2023-12-22
- Packages for admin services and messages have been extracted to a dedicated project which results in
  new package paths.
  Migration:
    - Renaming: `com.digitalasset.canton.xyz.admin` -> `com.digitalasset.canton.admin.xyz`
    - `com.digitalasset.canton.traffic.v0.MemberTrafficStatus` -> `com.digitalasset.canton.admin.traffic.v0.MemberTrafficStatus`
    - Some messages are moved from `api` to `admin`:
      - `SequencerConnection`: `com.digitalasset.canton.domain.api.v0` -> `com.digitalasset.canton.admin.domain.v0`
      - `SequencerConnections`: `com.digitalasset.canton.domain.api.v0` -> `com.digitalasset.canton.admin.domain.v0`

## Until 2023-12-15

## Until 2023-12-08

- Renamed `Unionspace` with `Decentralized Namespace`. Affects all classes, fields, options, and RPC endpoints with `unionspace` in their name.
- `BaseResult.store` returned by the `TopologyManagerReadServiceX` is now typed so that we can distinguish between authorized and domain stores.

## Until 2023-11-28

- Replaced `KeyOwner` with the `Member` trait in the `keys.private` and `owner_to_key_mappings.rotate_key` commands.
- Removed the deprecated `owner_to_key_mappings.rotate_key` command without the `nodeInstance` parameter.
- Removed the deprecated ACS download / upload functionality and `connect_ha` participant admin commands.
- Removed the deprecated `update_dynamic_parameters` and `set_max_inbound_message_size` domain admin commands.
- Removed the deprecated `acs.load_from_file` repair macro.
- v0.SignedContent is deprecated in favor of v1.SignedContent in SequencerService.
  Migration: field `SignedContent.signatures` becomes repeated

## Until 2023-11-21

- Split of the lines. From now on, snapshot will be 3.0.0-SNAPSHOT
