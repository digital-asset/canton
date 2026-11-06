# Runtime versions

We want to be cautious when adding new versions to the list of supported versions but eager to remove EOL
versions from our list of supported versions.

## Postgres

[Versioning policies](https://www.postgresql.org/support/versioning/)

All supported versions are supported.

As of Nov 2026, it means:
- Default: 18
- Supported: 15 to 18
- Additional nightly tests: 15, 16, and 17.

## Java

[Versioning policies](https://www.oracle.com/java/technologies/java-se-support-roadmap.html)

All supported LTS versions are supported, and compiled with the lowest LTS version.

As of August 2025, it means:

- Default: 21
- Supported: 17 and 21.
- Additional nightly tests: 17
- Compiled with target 17.
