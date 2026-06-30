
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

### New `GetCompletions` endpoint on the command completion service
#### Background
The command completion service only offered `CompletionStream`, which always filters by a single user and a non-empty set of parties. There was no way to stream completions across all parties.

#### Specific Changes
A new completion streaming endpoint is introduced: `GetCompletions` (gRPC) or `/commands/command-completions` (JSON API). This endpoint offers more flexible filtering semantics compared to the existing `CompletionStream`, `/commands/completions` endpoint. It filters by `parties` only; there is no user filtering:
- A non-empty `parties` filters to those parties and requires `ReadAs` (or `ActAs`) for each.
- An empty `parties` returns completions for all parties and requires `ReadAsAnyParty` (or `ActAsAnyParty`).

In all other respects it behaves like `CompletionStream`, which becomes deprecated.

#### Impact and Migration
This is an additive change. `CompletionStream` is unchanged, so no migration is required.

### Minor Improvements
- Added latency metrics for signing and decryption operations.
- Updated nix package to source from GHCR and bump dpm version to 1.0.20 for remote dar support
- Fixed a BFT ordering sequencer crash-recovery issue affecting freshly onboarded nodes. If such a node crashed and restarted while still within its onboarding start epoch, on restart it could attempt to recover the output module from a block below its durable lower bound (a block that was never stored by this node), which could leave the node stuck. The onboarding boundary block is now persisted with its BFT time to seed BFT-time computation across a restart, the node's own activation time is reconstructed when needed for crash recovery, and output-module recovery is clamped to the durable lower bound.
- LSU: improved handshake between a sequencer and its successor: the physical synchronizer id and sequencer id are now validated.

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

