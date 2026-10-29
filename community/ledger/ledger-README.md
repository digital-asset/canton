# ledger

Home of our reference ledger implementation (Sandbox) and various ledger related libraries.

## Logging

### Logging Configuration

The Sandbox and Ledger API Server use [Logback](https://logback.qos.ch/) for logging configuration.

### Log levels

As most Java libraries and frameworks, the Sandbox and Ledger API Server use INFO as the default logging level. This level is for minimal
and important information (usually only startup and normal shutdown events). INFO level logging should not produce
increasing volume of logging during normal operation.

WARN level should be used for transition between healthy/unhealthy state, or in other close to error scenarios.

DEBUG level should be turned on only when investigating issues in the system, and usually that means we want the trail
loggers. Normal loggers at DEBUG level can be useful sometimes (e.g. Daml interpretation).

## Metrics

Sandbox and Ledger API Server provide a couple of useful metrics:

### Sandbox and Ledger API Server

The Ledger API Server exposes basic metrics for all gRPC services and some additional ones.
<table>
<thead><tr><td>Metric Name</td><td>Description</td></tr>
<tbody>
<tr><td><pre>LedgerApi.com.daml.ledger.api.v1.$SERVICE.$METHOD</pre></td><td>A <i>meter</i> that tracks the number of calls to the respective service and method.
<tr><td><pre>CommandSubmission.failedCommandInterpretations</pre></td><td>A <i>meter</i> that tracks the failed command interpretations.
<tr><td><pre>CommandSubmission.submittedTransactions</pre></td><td>A <i>timer</i> that tracks the commands submitted to the backing ledger.
</tbody>
</table>

### Indexer
<table>
<thead><tr><td>Metric Name</td><td>Description</td></tr></thead>
<tbody>
<tr><td><pre>JdbcIndexer.processedStateUpdates</pre></td><td>A <i>timer</i> that tracks duration of state update processing.</td></tr>
<tr><td><pre>JdbcIndexer.lastReceivedRecordTime</pre></td><td>A <i>gauge</i> that returns the last received record time in milliseconds since EPOCH.</td></tr>
<tr><td><pre>JdbcIndexer.lastReceivedOffset</pre></td><td>A <i>gauge</i> that returns that last received offset from the ledger.</td></tr>
<tr><td><pre>JdbcIndexer.currentRecordTimeLag</pre></td><td>A <i>gauge</i> that returns the difference between the Indexer's wallclock time and the last received record time in milliseconds.</td></tr>
</tbody>
</table>

### Metrics Reporting

The Sandbox automatically makes all metrics available via JMX under the JMX domain `com.digitalasset.canton.platform.sandbox`.

When building an Indexer or Ledger API Server the implementer/ledger integrator is responsible to set up
a `MetricRegistry` and a suitable metric reporting strategy that fits their needs.

## Health Checks

### Ledger API Server health checks

The Ledger API Server exposes health checks over the [gRPC Health Checking Protocol][]. You can check the health of
the overall server by making a gRPC request to `grpc.health.v1.Health.Check`.

You can also perform a streaming health check by making a request to `grpc.health.v1.Health.Watch`. The server will
immediately send the current health of the Ledger API Server, and then send a new message whenever the health changes.

The ledger may optionally expose health checks for underlying services and connections; the names of the services are
ledger-dependent. For example, the Sandbox exposes two service health checks:

- the `"index"` service tests the health of the connection to the index database
- the `"write"` service tests the health of the connection to the ledger database

To use these, make a request with the `service` field set to the name of the service. An unknown service name will
result in a gRPC `NOT_FOUND` error.

[gRPC Health Checking Protocol]: https://github.com/grpc/grpc/blob/master/doc/health-checking.md

### Indexer health checks

The Indexer does not currently run a gRPC server, and so does not expose any health checks on its own.

In the situation where it is run in the same process as the Ledger API Server, the authors of the binary are encouraged
to add specific health checks for the Indexer. This is the case in the Sandbox and Reference implementations.

### Checking the server health in production

Every node runs an HTTP health server, by default on port `5071`. To check the health of a node in production, query
it with any HTTP client. It needs no gRPC tooling, and works with plain HTTP probes in Docker, Kubernetes and load
balancers.

You can change the bind address and port with `monitoring.http-health-server.address` and
`monitoring.http-health-server.port` in the node's configuration.

The server exposes two endpoints:

| Endpoint | Aliases | Meaning |
|---|---|---|
| `/health/live` | `/health/liveness` | The process is alive. If this fails, restart the node. |
| `/health/ready` | `/health/readiness`, `/health` | The node can serve requests. If this fails, stop sending it traffic. |

Both return `200 OK` when serving, `503 Service Unavailable` when not serving, and `500 Internal Server Error` if the
check itself fails. The response body lists the status of each component the check depends on, which is useful when
debugging an unhealthy node.

On the command line, you can check the health as follows (changing the address to match your node):

```shell
$ curl -fsS http://localhost:5071/health/ready
```

If your environment doesn't have `curl`, `wget` works as well:

```shell
$ wget -Y off -q -T 5 -O /dev/null http://localhost:5071/health/ready
```

Note that readiness reflects the state of the node's storage. During startup, the readiness endpoint returns `503`
until the database is reachable and all schema migrations have been applied, which can take a long time after an
upgrade. The liveness endpoint reports alive throughout startup, so a long migration does not cause liveness-based
restarts.

#### Kubernetes

Use `httpGet` probes against the HTTP health server:

```yaml
containers:
  - name: participant
    ports:
      - name: health
        containerPort: 5071
    livenessProbe:
      httpGet:
        path: /health/live
        port: health
      initialDelaySeconds: 60
      periodSeconds: 60
      failureThreshold: 5
      timeoutSeconds: 10
    readinessProbe:
      httpGet:
        path: /health/ready
        port: health
      periodSeconds: 5
      failureThreshold: 3
      timeoutSeconds: 10
```

When choosing timeouts, keep in mind that a pod stays unready for the whole duration of database migrations on startup.
Anything that waits for the pod to become ready, such as `helm install --wait`, `kubectl rollout status`, or the
deployment's `progressDeadlineSeconds`, needs a timeout that covers the longest migration you expect.

Do not point the liveness probe at the readiness endpoint. A node that is temporarily unready, for example because its
database is unavailable, would then be restarted instead of being taken out of the load balancer.

If you prefer gRPC, Kubernetes 1.27 and later can probe the node's gRPC health server (`monitoring.grpc-health-server`)
directly, without any additional binary, using `grpc` probes. Use the `liveness` service for the liveness probe and
the default service for the readiness probe. Be aware that the gRPC health server reports the node as serving before
schema migrations have completed, so it is a weaker readiness signal than the HTTP endpoint.

## gRPC and back-pressure

### RPC

Standard RPC requests should return with RESOURCE_EXHAUSTED status code to signal back-pressure. Envoy can be configured
to retry on these errors. We have to be careful not to have any persistent changes when returning with such an error as
the same original request can be retried on another service instance.

### Streaming

gRPC's streaming protocol has built-in flow-control, but it's not fully active by default. What it does it controls the
flow between the TCP/HTTP layer and the library so it builds on top of TCP's own flow control. The inbound flow control
is active by default, but the outbound does not signal back-pressure out of the box.

`AutoInboundFlowControl`: The default behaviour for handling incoming items in a stream is to automatically signal demand
after every `onNext` call. This is the correct thing to do if the handler logic is CPU bound and does not depend on other
reactive downstream services. By default it's active on all inbound streams. One can disable this and signal demand by
manually calling `request` to follow demands of downstream services. Disabling this feature is possible by calling
`disableAutoInboundFlowControl` on `CallStreamObserver`.

`ServerCallStreamObserver`: casting an outbound `StreamObserver` manually to `ServerCallStreamObserver` gives us access
to `isReady` and `onReadyHandler`. With these methods we can check if there is available capacity in the channel i.e.
we are safe to push into it. This can be used to signal demand to our upstream flow. Note that gRPC buffers 32Kb data
per channel and `isReady` will return false only when this buffer gets full.
