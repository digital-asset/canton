// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, BeforeAndAfterEach, OptionValues}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

class TraceContextTest extends AnyWordSpec with Matchers with OptionValues with BeforeAndAfterEach {

  var testTelemetrySetup: TestTelemetrySetup = _

  override def beforeEach(): Unit =
    testTelemetrySetup = new TestTelemetrySetup()

  override def afterEach(): Unit =
    testTelemetrySetup.close()

  private def spansAreEqual(span1: Span, span2: Span): Assertion = {
    val ctx1 = span1.getSpanContext
    val ctx2 = span2.getSpanContext
    assert(ctx1.getTraceId == ctx2.getTraceId && ctx1.getSpanId == ctx2.getSpanId)
  }

  "TelemetryContext" can {
    "be serialized by Java serialization" in {
      val rootSpan = testTelemetrySetup.tracer.spanBuilder("test").startSpan()
      val childSpan = testTelemetrySetup.tracer
        .spanBuilder("test")
        .setParent(TraceContext.empty.context.`with`(rootSpan))
        .startSpan()

      val emptyContext = TraceContext.empty
      val contextWithRootSpan = TraceContext(emptyContext.context.`with`(rootSpan))
      val contextWithChildSpan = TraceContext(emptyContext.context.`with`(childSpan))

      val byteOutputStream = new ByteArrayOutputStream()
      val outputStream = new ObjectOutputStream(byteOutputStream)
      outputStream.writeObject(emptyContext)
      outputStream.writeObject(contextWithRootSpan)
      outputStream.writeObject(contextWithChildSpan)
      outputStream.flush()
      outputStream.close()
      val bytes = byteOutputStream.toByteArray

      val inputStream = new ObjectInputStream(new ByteArrayInputStream(bytes))
      val ctx1 = inputStream.readObject().asInstanceOf[TraceContext]
      val ctx2 = inputStream.readObject().asInstanceOf[TraceContext]
      val ctx3 = inputStream.readObject().asInstanceOf[TraceContext]
      inputStream.close()

      Span.fromContextOrNull(ctx1.context) shouldBe null
      spansAreEqual(Span.fromContextOrNull(ctx2.context), rootSpan)
      spansAreEqual(Span.fromContextOrNull(ctx3.context), childSpan)
    }

    "deserialize and re-serialize a concrete W3C trace context" in {
      val traceParent = "00-6a61d0a2000000002ea86a24cff7cfcd-7358c6deaed4868f-01"
      val traceState = "dd=s:1;p:7358c6deaed4868f;t.dm:-1;t.tid:6a61d0a200000000;t.ksr:1"

      val headers = Map(
        W3CTraceContext.TraceparentHeader -> traceParent,
        W3CTraceContext.TracestateHeader -> traceState,
      )

      // deserialize the headers into a W3CTraceContext
      val w3c = W3CTraceContext.fromHeaders(headers).value
      w3c.parent shouldBe traceParent
      w3c.state.value shouldBe traceState

      // it exposes the same headers again
      w3c.asHeaders shouldBe headers

      // round-trip through a TraceContext and back preserves the span identifiers
      val traceContext = w3c.toTraceContext
      traceContext.traceId.value shouldBe "6a61d0a2000000002ea86a24cff7cfcd"
      traceContext.spanId.value shouldBe "7358c6deaed4868f"

      val roundTripped = traceContext.asW3CTraceContext.value
      roundTripped.parent shouldBe traceParent
      roundTripped.state.value shouldBe traceState
      roundTripped shouldBe w3c

      W3CTraceContext.extractHeaders(traceContext).map { case (k, v) =>
        HeaderName(k) -> v
      } shouldBe headers
    }

    "convert back and forth from W3C trace context" in {
      val rootSpan = testTelemetrySetup.tracer.spanBuilder("test").startSpan()
      val childSpan = testTelemetrySetup.tracer
        .spanBuilder("test")
        .setParent(Context.root().`with`(rootSpan))
        .startSpan()

      val emptyContext = TraceContext.empty
      val contextWithRootSpan = TraceContext(Context.root().`with`(rootSpan))
      val contextWithChildSpan = TraceContext(Context.root().`with`(childSpan))

      emptyContext.asW3CTraceContext shouldBe None
      Span.fromContextOrNull(W3CTraceContext("").toTraceContext.context) shouldBe null

      val rootW3c = contextWithRootSpan.asW3CTraceContext.value
      spansAreEqual(Span.fromContextOrNull(rootW3c.toTraceContext.context), rootSpan)

      val childW3c = contextWithChildSpan.asW3CTraceContext.value
      spansAreEqual(Span.fromContextOrNull(childW3c.toTraceContext.context), childSpan)
    }
  }
}
