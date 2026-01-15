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

### Minor Improvements
- Changed the path for `crypto.kms.session-signing-keys` (deprecated) to `crypto.session-signing-keys` so that session signing key configuration is no longer directly tied to a KMS. However, session signing keys can still only be enabled when using a KMS provider or when running with `non-standard-config=true`.
- Added a new configuration parameter for session signing keys, `toleranceShiftDuration`, and updated `cutOffDuration` to allow a zero duration.
- Ledger JSON Api changes:
  - extra fields in JSON objects are no longer tolerated,
  - All JSON values are optional by default upon decoding (this is not reflected in the openapi spec yet, but written comments should reflect the optionality),
- ApiRequestLogger now also used by Ledger JSON Api. Changes:
    - Redundant Request TID removed from logs.
    - Additional CLI options added: `--log-access`, `--log-access-errors`...
    - Additional config options added: `debugInProcessRequests`, `prefixGrpcAddresses`
- ParticipantRepairService.ExportAcsOld and ImportAcsOld are deprecated. Instead use ParticipantRepairService.ExportAcs and ImportAcs respectively as a direct replacement. For party replication use PartyManagementService.ExportPartyAcs and ImportPartyAcs instead.
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
- *BREAKING* The Postgres configuration of the indexer is separated from the Postgres configuration of the lapi server
  (`canton.participants.<participant>.ledger-api.postgres-data-source`).
  The new parameter `canton.participants.<participant>.parameters.ledger-api-server.indexer.postgres-data-source` should
  be used instead.
- Added network timeout and client_connection_check_interval for db operations in the lapi server and indexer to avoid
  hanging connections for Postgres (see PostgresDataSourceConfig). The defaults are 60 seconds network timeout and
  5 seconds client_connection_check_interval for the lapi server, and 20 seconds network timeout and
  5 seconds client_connection_check_interval for the indexer. These values can be configured via the new configuration parameters
  `canton.participants.<participant>.ledger-api.postgres-data-source.network-timeout` for network timeout of the lapi
  server and `canton.participants.<participant>.parameters.ledger-api-server.indexer.postgres-data-source.client-connection-check-interval`
  for the client_connection_check_interval of the indexer.
- We have changed the way that OffsetCheckpoints are populated to always generate at least one when an open-ended
  updates or completions stream is requested. An OffsetCheckpoint can have offset equal to the exclusive start for which
  the stream is requested. This ensures that checkpoints are visible even when there are no updates, and the stream was
  requested to begin exclusively from the ledger end.

### Preview Features
- preview feature

## Bugfixes

- Fixed a bug preventing automatic synchronization of protocol feature flags.
Automatic synchronization can be disabled by setting `parameters.auto-sync-protocol-feature-flags = false` in the participant's configuration object.

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

### Online party replication *breaking change*
The online party replication status command now returns a status in a very different, "vector-status" format
rather than the old "oneof" style.

Impacted Command:
- `participant.parties.get_add_party_status`

Impacted gRPC endpoint:
- `com.digitalasset.canton.admin.participant.v30.PartyManagementService.GetAddPartyStatus` response type

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

## update to GRPC 1.77.0

removes [CVE-2025-58057](https://github.com/advisories/GHSA-3p8m-j85q-pgmj) from security reports.


