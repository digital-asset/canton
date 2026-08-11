# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### Traffic Enforcement App
- Added `reject-multi-party-submissions` to the participant's traffic enforcement configuration.
  Multi-party submissions normally bypass traffic enforcement, since TEA accounts are bound to a
  single party. Setting this to `true` rejects them instead. Disabled by default.

  ```
  canton.participants.participant1.traffic-enforcement {
    enabled = true
    reject-multi-party-submissions = true
  }
  ```

### Topic A
Template for a bigger topic
#### Background
#### Specific Changes
#### Impact and Migration

### Minor Improvements
- Added the option to reject multi-party submissions under traffic enforcement on the Participant Node, instead of letting them bypass it. Disabled by default.
- CantonBFT now deletes unnecessary messages from previous views to prevent excessive memory usage during view changes.
- CantonBFT limits standalone mode to non-standard config enabled
- Fixed relative segment latency metric (CantonBFT)
- CantonBFT now logs more information about the view change process, including the reason for the view change and the new leader.
- CantonBFT: logs about backpressure and P2P that didn't provide useful information for operators have been removed
  or have seen their threshold lowered to DEBUG; other log messages have been improved.
- CantonBFT: the current epoch graph in the "BFT ordering" dashboard is now a time series rather than a mere stat.
- Fixed deserialization failure in AcsCommitmentCatchUpConfig
- Fixed an issue with `authenticationServiceChannel` to ensure it closes properly during shutdown.
- CantonBFT: fixed a security issue where a malformed retransmission request could crash a sequencer node
- CantonBFT: remove a deprecated gRPC header previously used internally for P2P authentication

#### Improved Sequencer Logging
On the sequencer, the log line mentioning all events in a block now also can contain the outcome of the event.
By setting `canton.sequencers.sequencer.parameters.enable-async-sequencer-logging = true`, the logging will be
moved to the end of the block processing, but will include the outcome of the events in the block. The default
remains `false` to preserve the current behavior.
Note that as part of this change, the sequencer-id of the traffic control metrics and of the block event processor
metrics dropped the superfluous leading "SEQ::" string.

#### Detailed Participant to Sequencer Connect Logging
More debug logging has been added to the participant connecting to a sequencer in order to debug a race condition
in which connecting appears to hang at times.

#### Configurable keepalive settings for CantonBFT's P2P server-side and more robust defaults
CantonBFT's P2P server-side now has configurable keepalive settings and defaults have been made more robust
against network infrastructure that may drop idle connections, leaving them in a zombie state.

### Preview Features
- preview feature

## Bugfixes
- Improved engine handing of `UnsupportedContractIdVersion`: disclosure from a future version of Canton could cause the engine to crash
  and fail submissions with an internal error. A `UnsupportedContractId` interpretation error is now returned instead.

### (YY-nnn, Risk): Title

#### Issue Description

#### Affected Deployments

#### Affected Versions

#### Impact

#### Symptom

#### Workaround

#### Likeliness

#### Recommendation

### Preserve `traceparent` information for traces with `tracestate` information

When serializing a trace in W3C format, the `traceparent` information was not preserved. This has been fixed.

However, a limitation still exists whereby CantonBFT discards the `tracestate` information.
This limitation will be addressed in a future release.

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

