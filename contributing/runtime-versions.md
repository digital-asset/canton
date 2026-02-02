# Runtime versions

We want to be cautious when adding new versions to the list of supported versions but eager to remove EOL
versions from our list of supported versions.

## Postgres

[Versioning policies](https://www.postgresql.org/support/versioning/)

All supported versions are supported.

As of August 2025, it means:
- Default: 17 (latest stable hoping for better performances)
- Supported: 14 to 17 (Postgres 13 will be EOL in November 2025, roughly when Canton 3.4 will be released)
- Additional nightly tests: 14, 15 and 16.

## Java

[Versioning policies](https://www.oracle.com/java/technologies/java-se-support-roadmap.html)

All supported LTS versions are supported, and compiled with the lowest LTS version.

As of August 2025, it means:

- Default: 21
- Supported: 17 and 21.
- Additional nightly tests: 17
- Compiled with target 17.
