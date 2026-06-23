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
- Connection pool metrics:
  - Add a `psid` label, populated if it is provided when connecting. This should be the case starting from the second connection to a synchronizer, or upon LSU.
  - Close the `connection-health` and `subscription-health` metrics associated to the `psid` when the pool is closed, instead of closing all the existing ones when the pool is started.
- Updated com.google.protobuf libs from 3.25.5 --> 3.25.9
- A call to `AcknowledgeSigned` with a timestamp before the upgrade time returns immediately, without any acknowledgement being done.
- Fix JDBC query for computing last activations: Under adverse conditions (data corruption) this query might called with empty inputs where it would have failed execution with PostgreSQL server version 14.
- (Potentially) *BREAKING*: Aggregatable submissions are now rejected eagerly to preserve bandwidth.
  This means that the submission error code `SEQUENCER_AGGREGATE_SUBMISSION_ALREADY_SENT` may now also
  be returned during the synchronous submission of the sequencer, as the state of the aggregation is also
  checked before ordering. In addition, the GRPC error code has been modified from `FAILED_PRECONDITION` to
  `ALREADY_EXISTS` to better reflect the nature of the error. Clients should be updated to handle this error
  code accordingly. Due to backwards compatibility, the old GRPC error code will be returned for PV35 and
  before on the async path, and the new capability must only be turned on when all nodes have been
  upgraded to a Canton version that supports this change. The new capability can be enabled using `canton.sequencers.seq.parameters.enable-reject-delivered-aggregations-on-pv-35 = MED`
  for mediators. This can be combined with the new configuration option of the mediator `canton.mediators.mymediator.parameters.delayed-verdict-sender.enabled = true`.
  Generally, the sequencer will send out the verdict after reaching the threshold. All subsequent sent verdicts are thrown away. The new option now allows threshold + extra verdicts to be sent immediately, while the rest of the mediators will wait a short amount of time. This allows to reduce the load on the sequencer by 30%, creating more capacity for other transactions.
- Fixed an issue that prevent external parties from being allocated on a participant with an offline root key via the Ledger API's `PartyManagementService.AllocateExternalParty` endpoint.

### Preview Features
- preview feature

## Bugfixes

### (YY-nnn, Risk): Title

#### Issue Description

#### Affected Deployments

#### Affected Versions

#### Impact

#### Symptom

#### Workaround

#### Likeliness

#### Recommendation

### (26-004, High): LSU: Missing synchronization between topology local copy and purging

#### Issue Description
Because of the lack of synchronization, it can happen that topology purging kicks in before the local copy of the topology state is finished, which result in incorrect topology state for the successor.

The issue can occur only when topology purging is enabled, which is not the case by default.

#### Affected Deployments
Participant nodes

#### Affected Versions
All versions before 3.5.6

#### Impact
Topology fork

#### Symptom

- Participant nodes issues warning/errors about missing topology transactions that were valid before the LSU.
- Submission of a transaction is rejected because of missing topology transactions that were valid before the LSU.

#### Workaround
Restore from backup and ensure topology purging is disabled

#### Likeliness
Exceptional

#### Recommendation
Upgrade to 3.5.6

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


## What's Coming

We are currently working on

