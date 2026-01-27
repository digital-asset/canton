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

- JSON Ledger API OpenAPI/AsyncAPI spec corrections
  - Fields not marked as required in the Ledger API `.proto` specification are now also optional in the OpenAPI/AsyncAPI specifications.
    If your client code is using code generated using previous versions of these specifications, it may not compile or function correctly with the new version. To migrate:
      - If you prefer not to update your code, continue using the previous specification versions as the JSON API server preserves backward compatibility.
      - If you want to use new endpoints, features or leverage the new less strict spec, migrate to the new OpenAPI/AsyncAPI specifications as follows:
        - Java clients: No changes are needed if you use the `OpenAPI Generator`. Otherwise, potentially optionality of fields should be handled appropriately for other code generators.
        - TypeScript clients: Update your code to handle optional fields, using the `!` or `??` operators as appropriate.
  - From Canton 3.5 onwards, OpenAPI/AsyncAPI specification files are suffixed with the Canton version (e.g., `openapi-3.5.0.yaml`).
  - Canton 3.5 is compatible with OpenAPI specification files from version 3.4.0 to 3.5.0 (inclusive).

### Minor Improvements
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

* Additional metrics for the ACS commitment processor: `daml.participant.sync.commitments.last-incoming-received`, `daml.participant.sync.commitments.last-incoming-processed`, `daml.participant.sync.commitments.last-locally-completed`, and `daml.participant.sync.commitments.last-locally-checkpointed`.

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

### Online party replication

Added the file-based online party replication command `participant.parties.add_party_with_acs_async` to
be used along with `participant.parties.export_party_acs` and instead of the sequencer-channel-based
`add_party_async` command.

The online party replication status command now returns status in a very different, "vector-status" format
rather than the old "oneof" style. This impacts the `participant.parties.get_add_party_status` command and
`com.digitalasset.canton.admin.participant.v30.PartyManagementService.GetAddPartyStatus` gRPC response type.

### Alpha Multi-Synchronizer Support

Adds a new participant node parameter, `alpha-multi-synchronizer-support` (Boolean).
- **Default (`false`):** Uses standard **Create** and **Archive** events.
- **Enabled (`true`):** Uses **Assign** and **Unassign** events.

This flag is required in multi-synchronizer environments to preserve the **reassignment counter** of a contract.
Using the default (Create events) resets this counter to zero.

Note: Multi-synchronizer support is currently in Alpha; most Ledger API consumers may not yet be compatible with
Assign/Unassign events. Only enable this if your application specifically requires non-zero reassignment counters
and can process these event types.

### Party replication repair console macro removal

The original party replication, which relied on a silent synchronizer, has been superseded by the offline party
replication process. As a result, the obsolete repair console macros associated with the old approach have
been removed.

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


