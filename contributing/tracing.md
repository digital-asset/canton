Tracing
=======

It can be difficult to make sense of log files when many concurrent requests are being processed as it can be unclear
which request has triggered which log message. To help with this we pass an `implicit` `TraceContext` object through
most methods dealing with requests. This `TraceContext` may contain trace information for the current request,
whose trace id will be placed alongside any messages logged while processing this request.

Keep in mind that the `TraceContext` is a game changer when it comes to resolving support requests.
Therefore, make an effort to pass the right trace context!

The trace information is also used for distributed span reporting.
We use the [OpenTelemetry](https://opentelemetry.io/) library, which supports a variety of reporting backends.
Check the Canton manual on how to expose the traces to Otlp or Zipkin.

# Passing `TraceContext`

You'll find that you need a `TraceContext` instance when calling any log method on the `NamedLogging` provided `logger`
or calling many other methods which already depend on having a trace context available. In most circumstances for functionality
processing requests is just to add a `(implicit traceContext: TraceContext)` argument list on your method. You may have
to add this to the callers of your method and continue up the call stack until finding an existing `traceContext`.

When there an underlying request does not exist (e.g. during startup/shutdown), it makes sense to leave the `TraceContext` empty.
You can do that in the following ways:

* Explicitly pass `TraceContext.empty` to the method that expects a trace context.
* Import `import TraceContext.Implicits.Empty._` to make the empty trace context available as implicit in the current scope.
* Mixin the `NoTracing` trait when no tracing details will ever be required.

# Creating a New `TraceContext`

If you are starting a new request you may have to start a new `TraceContext`. Typically, this is done by having your
class extend `Spanning` and call:

```scala
withNewTrace(functionFullName) { implicit traceContext => span =>
  span.setAttribute("foo", "bar")
  // your code
  span.addEvent("something happened")
  // your code
}
```

This operation will also start a new span, and it will be automatically ended when the enclosed code completes.
If the code returns a `Future`, the span will be closed on completion of the future and similarly for `EitherT` and `OptionT`.

You'll have access to a span object that allows you to optionally set attributes on the span
and add events at different points of the instrumented code, all of which will be made available in the reporter's UI.

# Creating a Child Span

Be conservative with the creation of new spans as an excess could have a negative impact on performance if they are all
being reported. It is generally preferable performance-wise to add events to an existing span than creating children spans.

That said, if you'd like to create a child span at some point deeper in the workflow, you can do so similarly extending `Spanning`
but using `withSpan` method that receives the current trace context implicitly from which the parent span of the
newly created span is taken.

```scala
withSpan("some description") { implicit traceContext => span =>
  // your code
}
```

# Using `Traced`

In some situations it may not be possible to pass the trace context in an implicit argument list (e.g. when using pekko streams).
For this, a `Traced[_]` wrapper is available with a variety of utilities for swapping between passing trace contexts implicitly and
passing in this wrapper.
However, in circumstances where types already exist to represent requests it may make more sense to simply append a trace context
field to these structures (e.g. `TimedTask`).

# Testing with `TraceContext`

Our `BaseTest` trait imports the `Empty` implicit `traceContext` so all tests using this will have an implicit trace context available.

When using `mockito` `when` or `verify` setups it will expect that all arguments in all argument lists are either
passed as instances or passed as argument matchers. `BaseTest` defines a `anyTraceContext` that will match `any` instance
of a `TraceContext` passed to a method.
