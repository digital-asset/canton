# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

INFO: Note that the "## Until YYYY-MM-DD" headers below should all be Wednesdays until 2pm CET to align with the weekly release schedule, i.e. if you add an entry effective at or after the first header, prepend the new date header that corresponds to the Wednesday after your change.

## Until 2026-03-25
### Minor Improvements
- Fixed the sequencer’s initialization key check to look for a `Protocol` key instead of a `SequencerAuthentication`
  key, because a sequencer does not require a key with `SequencerAuthentication` and the check may fail wrongly.

### Bug Fixes
- TopologyClient now returns the correct order of dynamic synchronizer parameter changes for the genesis timestamp.


## Until 2026-03-18
### Minor Improvements
- Internal: Replaced the sequencer's internal event signaller based on Pekko's `BroadcastHub` with an implementation with better runtime characteristics.
  The old event signaller can be turned on again with `canton.sequencers.<sequencer>.parameters.use-legacy-event-signaller = true`.

## Until 2026-03-11
### Minor Improvements
- JSON API: Synthetic `value` fields in oneOf wrapper types (e.g., `AssignCommand`, `UnassignCommand`, `Completion`) are now marked as required in the OpenAPI and AsyncAPI specifications, matching the actual API logic where these fields must always be present.
- Added optional parameters `confirmationResponseFactorO` and `confirmationResponsePatienceO` in `SubmissionRequestAmplification`.
  When sending confirmation responses, these parameters, if defined, override respectively the `factor` and `patience` parameters.
- Added a few metrics regarding submission requests amplification:
  - `attempt-sync-errors`: Count of send request attempts which receive a synchronous error
  - `amplified-attempts`: Count of send request attempts which are amplified
  - `amplification`: Rate and timings of submission request attempts to a sequencer
  - `no-connection-available`: Count of send attempts which are skipped because no connection is available
- Fixed a bug where reinitialized ACS commitments might fail to correctly persist the new commitments, causing mismatches to possibly reappear after a restart.

## Until 2026-03-04
### Minor Improvements
- *BREAKING* The
    - `/v2/updates` HTTP POST and websocket GET endpoints
    - `/v2/updates/flats` HTTP POST and websocket GET endpoints

  were incorrectly retuning LedgerEffects events (i.e., `CreatedEvent` and `ExercisedEvent`). They are now corrected to return
  AcsDelta (flat) events (i.e., `CreatedEvent` and `ArchivedEvent`).

### JSON Ledger API OpenAPI/AsyncAPI Specification Updates
We've corrected the OpenAPI and AsyncAPI specification files to properly reflect field requirements as defined in the Ledger API `.proto` files.
Additionally, specification files now include the Canton version in their filenames (e.g., `openapi-3.4.11.yaml`).

#### Impact and Migration
If you regenerate client code from these updated specifications, your code may require changes due to corrected field optionality. You have two options:
- **Keep using the old specification** - The JSON API server maintains backward compatibility with previous specification versions.
- **Upgrade to the new specification** - Update your client code to handle the corrected optional/required fields:
    - **Java (OpenAPI Generator)**: Code compiles without changes, but static analysis tools may flag nullability differences.
    - **TypeScript**: Handle optional fields using `!` or `??` operators as needed.

The JSON API server remains compatible with specification files from all 3.4.x versions (e.g., 3.4.9).

#### Specification bugfix
`GetUpdatesRequest.updateFormat` now appears marked as required in the OpenAPI/AsyncAPI specs even though it was previously optional.
This is a specification fix to align with the actual behavior of the API, which requires this field.
If your client code was previously omitting this field, you will need to update it to include a valid value.

### Bug Fixes
- Use an upper bound for max sequencing timestamp when reading the head state. This should prevent PostgreSQL executing
  the query for inflight aggregations with a sequential scan instead of using the available indices. In the past, this issue
  caused long startup times of the sequencer of more than 10 minutes.
- Fixed a bug where the storage config parameter `in-flight-aggregations-query-interval` was ignored.

## Until 2026-02-25

### Sequencer Inspection Service
A new service is available on the Admin API of the sequencer.
It provides an RPC that allows to query for traffic summaries of sequenced events.
Refer to the [traffic documentation](https://docs.digitalasset.com/subnet/3.4/howtos/operate/traffic.html) for more details.

### Minor Improvements
- A new field `paid_traffic_cost` exposes the traffic cost paid by the node on completion events and update events
  - On completions, the field contains the cost paid by the node for the submission of the transaction. May be 0 for failed transactions that did not incur any traffic cost.
  - On updates, the field contains the cost paid by the node for the submission of the transaction, if available on this node and to the querying parties. In particular, the cost is only available on the submitting node and when querying with a filter that includes submitting parties. The cost is available for Daml transactions and re-assignments. Not for topology transactions.
- The `sequencer-client.enable-amplification-improvements` flag now defaults to `true`.
- New connection pool:
  - The connections gRPC channels are now correctly using the defined client keep-alive configuration.
  - The connections gRPC channels are now configured with a `maxInboundMessageSize` set to `MaxInt` instead of the default 4MB (this will be improved in the future to use the dynamic synchronizer parameter `maxRequestSize`).
- Removed the excessive no longer needed debug logging around the sequencer event signaller
- Added `keep-alive-without-calls` and `idle-timeout` config values in the keep alive gRPC client configuration. See https://grpc.io/docs/guides/keepalive/#keepalive-configuration-specification for details.
  Note that when `keep-alive-without-calls` is enabled, `permit-keep-alive-without-calls` must be enabled on the server side, and `permit-keep-alive-time` adjusted to allow for a potentially higher frequency of keep alives coming from the client.
- Topology-Aware Package Selection (TAPS) refinement for handling inconsistent vetting states:
  - The algorithm now considers a party's package vetting state only for packages required by that party in the interpreted transaction. 
    It starts with a minimal set of restrictions derived from the command's root nodes and progressively accumulates more restrictions over a configurable number of passes.
    This iterative process increases the likelihood of finding a valid package selection set for the routing of the transaction.
  - The maximum number of TAPS passes can be set at the request-level via the optional `taps_max_passes` field in `Commands` or `PrepareSubmissionRequest` messages.
    If not specified, the default value is taken from the participant configuration via `participants.participant.ledger-api.topology-aware-package-selection.max-passes-default` (defaults to `3`).
    A hard limit is enforced by `participants.participant.ledger-api.topology-aware-package-selection.max-passes-limit` (defaults to `4`).

> [!IMPORTANT]
> `keep-alive-without-calls` can have a negative performance impact. Be cautious when turning it on, and in general prefer using `idle-timeout` when possible.

| Config                   | DefaultValue |
|--------------------------|--------------|
| keep-alive-without-calls | false        |
| idle-timeout             | 30 minutes   |

> [!TIP]
> The value for `idle-timeout` should be set lower than timeouts in the network stack between client and server.
> In particular, check the idle timeout configuration of Load Balancers. Defaults for [AWS ALB](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/application-load-balancers.html), [AWS NLB](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/update-idle-timeout.html), [GCP](https://docs.cloud.google.com/load-balancing/docs/https/request-distribution#http-keepalive-timeout).

Example:

Participant config:

```hocon
canton.participants.participant.sequencer-client.keep-alive-client.idle-timeout = 5 minutes
# And / Or
canton.participants.participant.sequencer-client.keep-alive-client.keep-alive-without-calls = true
canton.participants.participant.sequencer-client.keep-alive-client.keep-alive-time = 6 minutes
```

Sequencer config:

```hocon
# Must be enabled if keep-alive-without-calls is enabled on the client side
canton.sequencers.sequencer.public-api.keep-alive-server.permit-keep-alive-without-calls = true
canton.sequencers.sequencer.public-api.keep-alive-server.permit-keep-alive-time = 5 minutes
```
- Lowered the log level of certain warnings to INFO level, if validation of the confirmation request or confirmation response failed due to a race with a topology change.
- (Alpha): Sequencer throughput caps can now be modified while the sequencer is running. This is supported
  using the new Admin API rpc calls ``get_througput_cap`` and ``set_throughput_cap`` or by using the
  ``declarative`` configuration section of the sequencer node:
```
canton {
  parameters = {
    // note this is an alpha feature
    enable-alpha-state-via-config = true
    state-refresh-interval = 1s
  }
  sequencers {
    sequencer1 {
      declarative {
        throughput-cap = {
          "confirmation request" : {
            global-tps-cap = 45
            global-kbps-cap = 1000
            per-client-tps-cap = 7
            per-client-kbps-cap = 2500
          }
        }
      }
```
Note that you can start your process with multiple config files ``./bin/canton -c static.conf -c dynamic.conf``.
Canton will reload the config if any of the files changed (but only apply the declarative parts).
- Sequencer: Separated catchup from event buffer cache such that a catchup of a member does not lead to cache evictions
  of recent events.

### Bugfixes
- Switched the gRPC service `SequencerService.subscribe` and `SequencerService.downloadTopologyStateForInit` to manual
  control flow, so that the sequencer doesn't crash with an `OutOfMemoryError` when responding to slow clients.
- When the new connection pool is disabled using `sequencer-client.use-new-connection-pool = false`, the health of the
  connection pool is no longer reported as a component in `<node>.health.status` (before the fix, the connection pool
  component would report a "Not initialized" status).
- Fixed a race condition in mediator that could cause verdicts for invalid requests to not be emitted to ongoing inspection service streams.
