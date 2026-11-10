# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

<!--
Add each entry to the section and audience it affects. Write for application developers and node operators:
what changed, why it matters to them, and what to do. A bigger topic gets its own `####` heading with the
paragraphs Background, Specific Changes, and Impact and Migration as needed; small changes are bullets.
A breaking change also gets a bullet in the summary of breaking changes, and its column is set to `yes`.
Empty sections and these comments are removed when the release notes are polished.
-->

## Summary

_Write summary of release_

**Summary of breaking changes** (see [Breaking changes](#breaking-changes) for details and migration):

| Ledger API | Admin API and console | Protocol | Configuration |
| :--------- | :-------------------- | :------- | :------------ |
| no         | yes                   | no       | no            |

- Admin API and console:
  - A party-to-participant change that leaves fewer hosts with Submission or Confirmation permission than the
    party's confirmation threshold needs the force flag `FORCE_FLAG_ALLOW_CONFIRMING_THRESHOLD_CANNOT_BE_MET`.
  - `ImportPartyAcs` with a party rejects the import once the target participant has processed events past the
    party's activation.

## What's New

### New protocol version

### For application developers

### All node operators

### Participant node operators

#### Remote read support for Hybrid Party Replication (HyPR)

- The new `participant.parties.get_remote_acs` endpoint now allows one participant with a
  authorization to host a particular party to remotely read that party's ACS from another
  participant via a sequencer channel. Remote read support is disabled by default and requires
  the `alpha-acs-replication-support` configuration on both the source and target participant
  and the `alpha-sequencer-channel-support` configuration on at least one of the sequencer nodes
  in the synchronizer to be enabled.
- The new `participant.parties.get_party_freezing_time` is used along with `get_remote_acs`
  to determine the `asOf` timestamp.

#### Party ACS import for a Freezing participant

- For Hybrid Party Replication (HyPR), which a future 3.7 release will announce, `PartyManagementService.ImportPartyAcs`
  (console: `parties.import_party_acs`) with a `party_id` also accepts the import if the participant is Freezing for
  the party. It then requires the contract import mode `Validation`.
- Once connected to the synchronizer again, the participant moves its entry for the party from Freezing to Onboarding,
  and then, as after an import for an Onboarding participant, from Onboarding to Active once that is safe.

### Synchronizer operators

## Breaking changes

### Ledger API (application developers)

### All node operators

### Participant node operators

#### Traffic enforcement app: traffic top-ups from the ledger

The traffic enforcement app (TEA) can credit traffic top-ups published on the ledger by applications
that sell traffic (e.g. wallet providers).

- The new `canton-api-traffic-credit-v1` Daml package defines the `TrafficCreditReporter` interface. Its
  `PublishCredit` choice credits `trafficAmount` bytes to a TEA `accountId` and must be exercised with
  the authority of the paymaster in the interface view. TEA only ingests credits whose paymaster is one
  of its configured paymasters.
  The choice's `observers` argument can notify additional parties of the credit without granting them
  authority to publish credits.
  Implementing templates supply `trafficCreditReporter_publishCreditImpl`, which must validate account
  ids and positive traffic amounts. Validation can evolve with the implementation without changing
  the interface package.
- The DAR `canton-api-traffic-credit-v1-1.0.0.dar` is shipped in the `dars` directory of the release bundle,
  so applications can depend on it.
- Top-up ingestion is configured with the new setting
  `canton.participants.<participant>.traffic-accounting.traffic-enforcement-server.top-up`:
  ```
  top-up {
    type = daml-credit
    paymasters = ["paymaster::1220..."]
    // optional, defaults to the participant's pruning offset
    initial-offset-begin-exclusive = 42
  }
  ```
  The default is `type = disabled`. `paymasters` must not be empty, and the parties must be hosted on
  the participant.
  This replaces the settings `projection.top-up-paymasters` and
  `projection.initial-top-up-offset-begin-exclusive` of 3.7.0 and 3.7.1, which have been removed.
  Once an ingestion offset has been persisted, it takes precedence over
  `initial-offset-begin-exclusive`. Changing that setting does not reprocess transactions at or
  before the persisted offset.
- When Daml top-up is enabled, automatic upload and vetting of the `canton-api-traffic-credit-v1` DAR
  is controlled by the existing
  `canton.participants.<participant>.parameters.admin-workflow.auto-load-dar` setting (enabled by default).
  When enabled, the participant uploads the DAR before starting TEA and vets it on every synchronizer
  it connects to. This setting also controls automatic upload and vetting of other admin-workflow DARs,
  independently of Daml top-ups. Enabling Daml top-ups does not override this setting.
  To manage these DARs manually, disable this setting and upload and vet the traffic-credit DAR
  and any other required admin-workflow DARs. If the traffic-credit DAR is missing while automatic
  loading is disabled, the participant logs a warning.
- A `PublishCredit` exercise whose arguments or acting parties do not match what TEA expects is not
  credited and is reported with the error code `TRAFFIC_TOP_UP_NOT_CREDITED`, which carries the
  `update_id` and `offset` of the credit.

#### Confirmation threshold check counts only confirming hosts

- A party-to-participant change is rejected with `TOPOLOGY_CONFIRMING_THRESHOLD_CANNOT_BE_REACHED` if it leaves fewer
  hosts with Submission or Confirmation permission than the party's confirmation threshold, unless the force flag
  `FORCE_FLAG_ALLOW_CONFIRMING_THRESHOLD_CANNOT_BE_MET` is set.
- Since 3.4.4, the check also counted hosts with Observation permission, which never confirm. For example, a host
  could downgrade itself to Observation without the force flag, and every request that needs the party's confirmation
  then timed out until the party lowered its threshold.
- To downgrade a host, lower the threshold first, or in the same change. A party without hosts with Submission or
  Confirmation permission, and the removal of a party's mapping, are not checked.

#### Party ACS import rejected after the party's activation

- `PartyManagementService.ImportPartyAcs` (console: `parties.import_party_acs`) with a `party_id` now rejects the
  import with `PARTY_ACS_IMPORT_AFTER_ACTIVATION` once the target participant has processed events past the party's
  activation. This happens if the target participant stays connected while the party authorizes the hosting.
- Since the activation, the target participant may have processed archivals or unassignments of contracts in the
  snapshot. Previously, the import then failed midway, or made archived contracts active again if their archivals
  had been removed.
- Disconnect the target participant before the party authorizes the hosting, as the offline party replication steps
  require. If it has already processed the activation, restore it from a backup taken before the activation and
  import again.
- The force flag `IMPORT_PARTY_ACS_FORCE_FLAG_ALLOW_IMPORT_AFTER_ACTIVATION` in the new field `force_flags`
  (console: `forceFlags = Set(ImportPartyAcsForceFlag.AllowImportAfterActivation)`) skips the check. Set it only if
  no contract in the snapshot has been archived or unassigned since the activation.

### Synchronizer operators

## Deprecations

<!-- The deprecated item, its replacement, and when it will be removed. -->

## Minor Improvements

- The `Request-Timeout` header of JSON Ledger API requests now also accepts a time unit, for example `15s`. Values without a time unit are still interpreted as milliseconds.

### Ledger API

### All node operators

### Participant node operators

### Synchronizer operators

#### CantonBFT
- Observability: example Grafana dashboards have been upgraded to the latest Grafana and V2 format; also, some P2P logs have been cleaned up.

## Security and Bug Fixes

<!--
Advisories first, then the other fixes as a bullet list, including dependency updates for security vulnerabilities
(e.g. "Dependency updates to address security vulnerabilities: jackson 2.22.4, gRPC 1.85").
-->

<!--
Advisory for a bug fix with a severity, e.g. (25-012, Low). For a Critical or High security fix, only once the
fix is deployed on MainNet:

### (YY-nnn, Risk): Title

#### Issue Description

#### Affected Deployments

#### Affected Versions

#### Impact

#### Symptom

#### Workaround

#### Likeliness

#### Recommendation
-->

- Offline party replication: The party ACS import now considers only the imported party's own onboarding mapping,
  not those of other parties of the same namespace. If the target knows no onboarding mapping of the imported party
  itself, the import fails with `PARTY_ACS_IMPORT_MISSING_ONBOARDING_MAPPING`. Before, the target could clear the
  party's onboarding flag before all requests in flight at the party's onboarding were decided.
- Party replication no longer reads the mapping of another party of the same namespace whose identifier starts with
  the party's identifier, e.g. `party-10` for `party-1`. Before, the onboarding flag clearance could leave the party's
  flag set or clear the other party's flag too early, and online party replication could check the wrong mapping.
- Offline party replication: The automatic onboarding flag clearance no longer stalls when the first time the
  target participant observes is the safe time itself, or when its clearance is rejected because another change of
  the party's hosting took effect first, e.g. with two targets onboarding the party in parallel. Before, the flag
  stayed set until a reconnect, a restart or a manual `clear_party_onboarding_flag`.
- `clear_party_onboarding_flag` no longer leaves a pending onboarding clearance behind when called after the flag
  is cleared, as the documented polling does. Before, a later ACS import of the party, e.g. after offboarding it
  and onboarding it again, failed with `INVALID_STATE_PARTY_MANAGEMENT_ERROR` (`ConflictingPendingOperationError`),
  and each reconnect ran a needless clearance attempt. Records left behind by earlier versions are deleted on the
  next reconnect, or when the participant stops hosting the party.
- During an announced synchronizer upgrade, the onboarding flag clearance no longer logs its waits as warnings.
- Sequencer node high CPU and memory usage after a few days of running due to `ThreadLocalMap` retaining
  stale state has been addressed.

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
