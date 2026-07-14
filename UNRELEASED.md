# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

This is a maintenance release that brings few minor improvements.

## What’s New

### Minor Improvements
- Report indexer as unhealthy after crash until at least one uncommitted update from before crash was processed or there are no uncommitted updates from before the crash. This puts a participant in unhealthy status if indexer enters crash loop due to problematic update in the queue.
- An internal change is introduced which grants `ReadAsAnyParty` permissions to the traffic enforcement service. This removes the need to use non-standard config options to run local traffic enforcement with auth enabled:
  ```
  canton.participants.<participant_name>.ledger-api.admin-token-config.act-as-any-party-claim=true
  canton.participants.parameters.non-standard-config=true
  ```

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
