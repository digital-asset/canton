# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

## Summary

_Write summary of release_

## What’s New

### Removal of protocol version 34
Protocol version 34 is not supported anymore. This release supports protocol versions 35, 36 and 37.

### Topic A
Template for a bigger topic
#### Background
#### Specific Changes
#### Impact and Migration

### Initialization changes for sequencers and mediators

Rather than genenerating the onboarding topology transactions during node initialization and storing these in the authorized store,
sequencers and mediators now skip this step and generate them when needed.

This ensures these topology transactions are generated in the right protocol version (which is not known during initialization).

Practically, this means that instead of the `node.topology.transactions.identity_transactions()` console command,
the `node.topology.transactions.generate_onboarding_transactions(protocolVersion)` command should be used instead instead.

If you are using an offline root key, you cannot call this
(but you would have already been signing and providing the transactions manually using the offline key).

### Key management operations are now bound to a synchronizer

The following console commands now take an optional `synchronizerId` argument:

- `node.topology.owner_to_key_mappings.add_key`
- `node.topology.owner_to_key_mappings.add_keys`
- `node.topology.owner_to_key_mappings.remove_key`
- `node.topology.owner_to_key_mappings.rotate_keys`
- `node.keys.secret.rotate_kms_node_key`
- `node.keys.secret.rotate_node_key`
- `node.keys.secret.rotate_node_keys`

If omitted, we will try to auto-detect the synchronizer instead.
This will only succeed when there is exactly a single registered and connected synchronizer.

It is considered a security best practice to have a distinct key per synchronizer.

### Minor Improvements
- participant_id label is added onto participant metrics
- To maintain ledger consistency when importing repair events, repair events are now rejected if the last persisted event was a topology event. If this happens, reconnect to the synchronizer to move the record time. If cannot reconnect, use the `forceRepairWhenTopologyTransactionAtLedgerEnd` flag. Using the force flag can corrupt the data in the system.
- Deprecated configuration settings: `canton.participants.<participant>.parameters.ledger-api-server.indexer.use-weighted-batching` and `canton.participants.<participant>.parameters.ledger-api-server.indexer.submission-batch-insertion-size`. These are no longer supported.
- Support for OTLP remote metrics reporting, including optional OAuth2 Client Credentials authentication
  ```
  canton.monitoring.metrics.reporters = [{
    type = otlp
      endpoint = "http://localhost:4317"
      protocol = grpc
      auth {
        type = oauth-client-credentials
        token-url = "http://localhost:8080/token"
        client-id = "test-client"
        client-secret = "test-secret"
      }
   }]
  ```

### Preview Features
- preview feature

## Bugfixes
- bump da-base-image to 1.0.14

### (YY-nnn, Risk): Title

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
