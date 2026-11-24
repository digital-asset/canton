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

#### ACS replication via `get_remote_acs`

- Sequencer channel members need to be on this version of 3.7 to be able to establish a
  secure sequencer channel with each other for ACS replication via `get_remote_acs` .

### Synchronizer operators

## Breaking changes

### Ledger API (application developers)

### All node operators

### Participant node operators

### Synchronizer operators

## Deprecations

<!-- The deprecated item, its replacement, and when it will be removed. -->

## Minor Improvements
- Upgraded bouncy castle to 1.87 to cover off security advisory BDSA-2026-40175

### Ledger API

### All node operators

### Participant node operators

### Synchronizer operators

## Security and Bug Fixes

### JDK Base Image Critical Fixes
- JDK base image has been upgraded to 21.0.13.1_2, incorporating several key security patches

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

- Fixed a logical synchronizer upgrade (LSU) losing the traffic purchased for a member that was not yet registered
  with the sequencer: `traffic_control.get_lsu_state` now includes these members, and `traffic_control.set_lsu_state`
  restores their traffic on the new sequencers. Both the old and the new sequencers must run this version when the
  traffic state is transferred.

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
