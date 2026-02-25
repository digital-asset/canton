# Release of Canton CANTON_VERSION

Canton CANTON_VERSION has been released on RELEASE_DATE.

INFO: Note that the "## Until YYYY-MM-DD" headers below should all be Wednesdays until 2pm CET to align with the weekly release schedule, i.e. if you add an entry effective at or after the first header, prepend the new date header that corresponds to the Wednesday after your change.

## Until 2026-02-25

### JSON Ledger API OpenAPI/AsyncAPI Specification Updates
We've corrected the OpenAPI and AsyncAPI specification files to properly reflect field requirements as defined in the Ledger API `.proto` files. Additionally, specification files now include the Canton version in their filenames (e.g., `openapi-3.4.11.yaml`).

#### Impact and Migration
If you regenerate client code from these updated specifications, your code may require changes due to corrected field optionality. You have two options:
- **Keep using the old specification** - The JSON API server maintains backward compatibility with previous specification versions.
- **Upgrade to the new specification** - Update your client code to handle the corrected optional/required fields:
    - **Java (OpenAPI Generator)**: Code compiles without changes, but static analysis tools may flag nullability differences.
    - **TypeScript**: Handle optional fields using `!` or `??` operators as needed.

The JSON API server remains compatible with specification files from all 3.4.x versions (e.g., 3.4.9).

### Sequencer Inspection Service
A new service is available on the Admin API of the sequencer.
It provides an RPC that allows to query for traffic summaries of sequenced events.
Refer to the [traffic documentation](https://docs.digitalasset.com/subnet/3.4/howtos/operate/traffic.html) for more details.

### Minor Improvements
- The `sequencer-client.enable-amplification-improvements` flag now defaults to `true`.
- New connection pool:
  - The connections gRPC channels are now correctly using the defined client keep-alive configuration.
  - The connections gRPC channels are now configured with a `maxInboundMessageSize` set to `MaxInt` instead of the default 4MB (this will be improved in the future to use the dynamic synchronizer parameter `maxRequestSize`).
- Removed the excessive no longer needed debug logging around the sequencer event signaller
- Added `keep-alive-without-calls` and `idle-timeout` config values in the keep alive gRPC client configuration. See https://grpc.io/docs/guides/keepalive/#keepalive-configuration-specification for details.
  Note that when `keep-alive-without-calls` is enabled, `permit-keep-alive-without-calls` must be enabled on the server side, and `permit-keep-alive-time` adjusted to allow for a potentially higher frequency of keep alives coming from the client.

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

### Bugfixes
- Switched the gRPC service `SequencerService.subscribe` and `SequencerService.downloadTopologyStateForInit` to manual
  control flow, so that the sequencer doesn't crash with an `OutOfMemoryError` when responding to slow clients.
- When the new connection pool is disabled using `sequencer-client.use-new-connection-pool = false`, the health of the
  connection pool is no longer reported as a component in `<node>.health.status` (before the fix, the connection pool
  component would report a "Not initialized" status).
