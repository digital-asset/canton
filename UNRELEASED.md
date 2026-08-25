# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

NOTE: we must put in a release note here announcing that Canton BFT is GA for private sync.

_Write summary of release_

## What’s New

### Topic A
Template for a bigger topic
#### Background
#### Specific Changes
#### Impact and Migration

### Minor Improvements
- Fixed shutdown of subscription queue when client is cancelled or disconnected
- Fixed an issue with the P2P grpc connection channel to ensure it closes properly during shutdown.
- Added various metrics for the Traffic Enforcement App performing local traffic accounting on validator nodes
- The JSON Ledger API now accepts `traceparent` and `tracestate` request header names case-insensitively, in compliance with the specification.

| Metrics Name                            | Type      | Description                                                                                                                                           |
|-----------------------------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `balance-lookups`                       | Counter   | The number of times the system has looked up an account balance.                                                                                      |
| `insufficient-balance-rejections`       | Counter   | The number of times the system has rejected an action due to insufficient traffic balance.                                                            |
| `allowed-submission-on-lookup-failures` | Counter   | The number of times the system has allowed a submission to proceed despite a failure to fetch traffic information.                                    |
| `enforcement-check-duration`            | Histogram | The time taken to perform traffic enforcement checks, including balance lookups and decision making.                                                  |
| `projection-timestamp`                  | Gauge     | The timestamp of the latest consumed event by traffic enforcement projection, indicating how far the system has processed traffic enforcement events. |
| `projection-offset`                     | Gauge     | The latest saved offset of the traffic enforcement projection.                                                                                        |

- Updated Docker base image to 1.0.13, which updates gRPC health probe to v0.4.55.

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
