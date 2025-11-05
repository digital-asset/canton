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

### Preview Features
- preview feature

## Bugfixes

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

## Compatibility

The following Canton protocol and Ethereum sequencer contract versions are supported:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Canton protocol versions   | PROTOCOL_VERSIONS          |

Canton has been tested against the following versions of its dependencies:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Java Runtime               | JAVA_VERSION               |
| Postgres                   | POSTGRES_VERSION           |
