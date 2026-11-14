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
- The `canton-participant`, `canton-mediator`, and `canton-sequencer` Docker images now enable the HTTP readiness endpoint (`/health/ready` on port 5071) by default.
- The `HEALTHCHECK` of the `canton-participant` Docker image now uses the HTTP readiness endpoint (`/health/ready` on port 5071) instead of the gRPC health service.
  Readiness is now only reported once database migrations and storage initialization have completed, so after an upgrade with long-running migrations the container stays unhealthy for longer.
  Tooling that waits for the container to become healthy (e.g., `docker compose up --wait` or `condition: service_healthy`) may need longer timeouts.
- Bump base docker image to [1.0.19](https://github.com/canton-network/canton-base-images/releases/tag/v1.0.19) to address security vulnerabilities.

### Preview Features
- preview feature

## Bugfixes
- Sequencer node: fixed a resource leak, causing a spike of # of threads, CPU and memory consumption affecting the node's
  ability to keep up with the rest of synchronizer, requiring an imminent restart of the node every few days.
- Participant node: when a participant shuts down or becomes passive, the ledger API indexer no longer logs
  "Consumer terminated with a failure" at WARN level for the aborted post processing.

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
