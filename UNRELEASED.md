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
- Ledger JSON Api changes:
  - extra fields in JSON objects are no longer tolerated,
  - All JSON values are optional by default upon decoding (this is not reflected in the openapi spec yet, but written comments should reflect the optionality),
- ApiRequestLogger now also used by Ledger JSON Api. Changes:
    - Redundant Request TID removed from logs.
    - Additional CLI options added: `--log-access`, `--log-access-errors`...
    - Additional config options added: `debugInProcessRequests`, `prefixGrpcAddresses`
- ParticipantRepairService.ExportAcsOld and ImportAcsOld are deprecated. Instead use ParticipantRepairService.ExportAcs and ImportAcs respectively as a direct replacement. For party replication use PartyManagementService.ExportPartyAcs and ImportPartyAcs instead.
- Removed `packageDependencyCache` from `caching` configuration.
- The `generateExternalPartyTopology` endpoint on the Ledger API now returns a single `PartyToParticipant` topology transaction to onboard the party.
The transaction contains signing threshold and signing keys. This effectively deprecate the usage of `PartyToKeyMapping`.
For parties with signing keys both in `PartyToParticipant` and `PartyToKeyMapping`, the keys from `PartyToParticipant` take precedence.
- Batching configuration now allows setting different parallelism for pruning (currently only for Sequencer pruning):
  New option `canton.sequencers.sequencer.parameters.batching.pruning-parallelism` (defaults to `2`) can be used
  separately from the general `canton.sequencers.sequencer.parameters.batching.parallelism` setting.

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
