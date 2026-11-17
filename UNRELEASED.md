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
| no         | no                    | no       | no            |

## What's New

### New protocol version

### For application developers

### All node operators

### Participant node operators

#### Remote read of a party's ACS (alpha)

- Invoking `get_remote_acs` again with the same arguments while a previous invocation has not
  completed (for example after the previous invocation has failed or its client has disconnected)
  restarts the remote read from beginning (reusing an existing sequencer channel agreement if
  available). The previous invocation, if still running, fails.
- Cancelling a `get_remote_acs` invocation stops the corresponding ACS replication. The
  remote read can be restarted as described above.
- `canton-builtin-admin-workflow-acs-replication-alpha.dar` is introduced as stable

### Synchronizer operators

#### Sequencer channel pool size and TTL limits (alpha)

- Sequencers with `alpha-sequencer-channel-support` enabled now limit sequencer channels:
  `max-active-sequencer-channels` (default `10`) caps how many channels a sequencer serves at a time.
  The new `sequencer-channel-ttl` (default `10m`) closes channels when their TTL expires and
  `max-metadata-waiting-time` (default `2m`) closes pending connections that haven't sent metadata in time.
  When the `max-active-sequencer-channels` limit is reached, new channels are rejected
  with the retryable error `SEQUENCER_CHANNEL_POOL_CAPACITY_EXHAUSTED`.
  Online party replication and `get_remote_acs` retry such rejections, and reconnect automatically
  when a channel is closed by the TTL.

## Breaking changes

### Ledger API (application developers)

### All node operators

### Participant node operators

- **Alleviate dangerous unvetting on Admin API DAR disable**: For avoiding unexpected behavior, the Admin API PackageService (`com.digitalasset.canton.admin.participant.v30.PackageService.UnvetDar`) is adapted
  to only unvet the main package of a DAR by default, in contrast with the previous behavior which by default unvetted all packages in the DAR.
  The old behavior can be toggled by setting the new request field `UnvetDarRequest.unvet_scope` to `UnvetScope.UNVET_SCOPE_ALL_DAR_PACKAGES`.
  Additionally, the associated Canton console command (`<participant>.dars.vetting.disable`) is adapted accordingly.

### Synchronizer operators

## Deprecations

<!-- The deprecated item, its replacement, and when it will be removed. -->

## Minor Improvements
- Previously `canton.remote-participants.<remoteParticipant>.token` was used for both ledger API and admin API commands in the Canton Console.
  Now, an optional `canton.remote-participants.<remoteParticipant>.admin-api-token` can be specified to use a different token for admin API
  commands in case that admin API auth differs from ledger API auth.

### Ledger API

### All node operators

- New console command `topology.transactions.export_single_to_file` and new optional `password` parameter of
  `topology.transactions.load_single_from_file`: export and load a signed topology transaction, optionally encrypted
  with a password by the console.

### Participant node operators
- The initialization of the Ledger API indexer after onboarding to a synchronizer needs much less memory and should also complete much faster.
  In testing, initializing the Ledger API indexer after onboarding the a participipant to a synchronizer with 10 million parties took ~90 seconds
  after the initial topology import.
- Improved the performance of Ledger API pruning queries. Contract pruning candidates are assembled with more but simpler queries with the help of temporary tables to ensure good query plans.

### Synchronizer operators
- The traffic state of members that traffic was purchased for before they joined the synchronizer (protocol version 37
  and up) can now be read back, including the serial needed for the next purchase. Set the new field
  `include_onboarding_members` of `SequencerAdministrationService.TrafficControlState` requests, or the new parameter
  `includeOnboardingMembers` of the sequencer console commands `traffic_control.traffic_state_of_members`,
  `traffic_state_of_members_approximate`, `traffic_state_of_members_at_timestamp`, `traffic_state_of_all_members` and
  `last_traffic_state_update_of_members` to `true`. Both default to false, which keeps the existing behavior unchanged.

## Security and Bug Fixes
- Increase google-cloud-kms version to 2.105.0 to address vulnerability CVE-2026-119078

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

- Fixed spurious `Locked connection was lost, trying to rebuild` warnings and unnecessary reconnects of DB locked connections
  on busy nodes: a read-only check that cannot run because the connection is in use no longer counts towards
  `replication.connection-pool.connection.max-inconclusive-read-only-checks`.

### CantonBFT

- Fixed race conditions identified in P2P connection teardown.

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
