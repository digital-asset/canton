# Release of Canton 2.10.2

Canton 2.10.2 has been released on July 23, 2025. You can download the Daml Open Source edition from the Daml Connect [Github Release Section](https://github.com/digital-asset/daml/releases/tag/v2.10.2). The Enterprise edition is available on [Artifactory](https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-2.10.2.zip).
Please also consult the [full documentation of this release](https://docs.daml.com/2.10.2/canton/about.html).

## Summary

This is a maintenance release that provides minor stability and performance improvements, and resolves a dependency issue with the KMS Driver artifacts.

## until 2025-07-23 (Exclusive)
- OTLP trace export configuration has been extended with several new parameters allowing connection to OTLP servers,
  which require more elaborate set-up:
    - `trustCollectionPath` should point to a valid CA certificate file. When selected a TLS connection
      is created instead of an open-text one.
    - `additionalHeaders` allows specifying key-value pairs that are added to the HTTP2 headers on all trace exporting
      calls to the OTLP server.
    - `timeout` sets the maximum time to wait for the collector to process an exported batch of spans.
      If unset, defaults to 10s.

## What’s New

### Improved Package Dependency Resolution

The package dependency resolver, which is used in various topology state checks and transaction processing is improved as follows:
  - The underlying cache is now configurable via `canton.parameters.general.caching.package-dependency-cache`.
    By default, the cache is size-bounded at 10000 entries and a 15-minutes expiry-after-access eviction policy.
  - The parallelism of the DB package fetch loader used in the package dependency cache
    is bounded by the `canton.parameters.general.batching.parallelism` config parameter, which defaults to 8.

### Contract Prefetching

Contract prefetching is now also supported for createAndExercise command. On top of that, we now support recursive prefetching,
which allows to prefetch also referenced contract ids. The default max prefetching level is 3 and can be configured using
canton.participants.participant.ledger-api.command-service.contract-prefetching-depth = 3

### Resolve KMS Driver Artifact Dependency Issues

The `kms-driver-api` and `kms-driver-testing` artifacts declared invalid dependencies in the Maven `pom.xml` files, which caused issues in fetching those artifacts. The declared invalid dependencies have been resolved.

## Compatibility

The following Canton protocol versions are supported:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Canton protocol versions   | 5, 7          |

Canton has been tested against the following versions of its dependencies:

| Dependency                 | Version                    |
|----------------------------|----------------------------|
| Java Runtime               | OpenJDK 64-Bit Server VM Zulu11.72+19-CA (build 11.0.23+9-LTS, mixed mode)               |
| Postgres                   | Recommended: PostgreSQL 12.22 (Debian 12.22-1.pgdg120+1) – Also tested: PostgreSQL 11.16 (Debian 11.16-1.pgdg90+1), PostgreSQL 11.16 (Debian 11.16-1.pgdg90+1), PostgreSQL 13.21 (Debian 13.21-1.pgdg120+1), PostgreSQL 13.21 (Debian 13.21-1.pgdg120+1), PostgreSQL 14.18 (Debian 14.18-1.pgdg120+1), PostgreSQL 14.18 (Debian 14.18-1.pgdg120+1), PostgreSQL 15.13 (Debian 15.13-1.pgdg120+1), PostgreSQL 15.13 (Debian 15.13-1.pgdg120+1)           |
| Oracle                     | 19.20.0             |

