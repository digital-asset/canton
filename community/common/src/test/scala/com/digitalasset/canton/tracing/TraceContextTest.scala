// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import java.nio.file.Path
import com.digitalasset.canton.util.MessageRecorder
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTestWordSpec, HasTempDirectory}
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import org.scalatest.{Assertion, BeforeAndAfterEach}

import scala.annotation.nowarn

@SuppressWarnings(
  Array(
    "org.wartremover.warts.TraversableOps",
    "org.wartremover.warts.Var",
    "org.wartremover.warts.Null",
  )
)
@nowarn("msg=match may not be exhaustive")
class TraceContextTest extends BaseTestWordSpec with HasTempDirectory with BeforeAndAfterEach {
  val recordFile: Path = tempDirectory.resolve("recorded-test-data")
  val recorder = new MessageRecorder(DefaultProcessingTimeouts.testing, loggerFactory)

  var testTelemetrySetup: TestTelemetrySetup = _

  override def beforeEach(): Unit = {
    testTelemetrySetup = new TestTelemetrySetup()
  }

  override def afterEach(): Unit = {
    testTelemetrySetup.close()
  }

  private def spansAreEqual(span1: Span, span2: Span): Assertion = {
    val ctx1 = span1.getSpanContext
    val ctx2 = span2.getSpanContext
    assert(ctx1.getTraceId == ctx2.getTraceId && ctx1.getSpanId == ctx2.getSpanId)
  }

  "TelemetryContext" can {
    "be serializable by message recorder" in {
      val rootSpan = testTelemetrySetup.tracer.spanBuilder("test").startSpan()
      val childSpan = testTelemetrySetup.tracer
        .spanBuilder("test")
        .setParent(TraceContext.empty.context.`with`(rootSpan))
        .startSpan()

      val emptyContext = TraceContext.empty
      val contextWithRootSpan = TraceContext(emptyContext.context.`with`(rootSpan))
      val contextWithChildSpan = TraceContext(emptyContext.context.`with`(childSpan))

      recorder.startRecording(recordFile)

      recorder.record(emptyContext)
      recorder.record(contextWithRootSpan)
      recorder.record(contextWithChildSpan)

      recorder.stopRecording()

      val List(ctx1, ctx2, ctx3) = MessageRecorder.load[TraceContext](recordFile, logger)

      Span.fromContextOrNull(ctx1.context) shouldBe null
      spansAreEqual(Span.fromContextOrNull(ctx2.context), rootSpan)
      spansAreEqual(Span.fromContextOrNull(ctx3.context), childSpan)
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

    // If the trace has no fields set and `tc.toProtoV0.toByteArray` is used, the trace context will serialize to an
    // empty byte array. This is problematic as some databases will treat an empty byte array as null for their blob
    // columns but our table definitions typically expect non-null for the trace context column value.
    // This is not an issue when serializing a `VersionedTraceContext` but to not regress we have this unit test.
    "won't be serialized to an empty ByteArray" in {
      val res = TraceContext.empty.toByteArray(ProtocolVersion.latestForTest)
      val empty = new Array[Byte](0)
      res should not be empty
    }

    "serialization roundtrip preserves equality" in {
      val rootSpan = testTelemetrySetup.tracer.spanBuilder("test").startSpan()
      val childSpan = testTelemetrySetup.tracer
        .spanBuilder("equality")
        .setParent(TraceContext.empty.context.`with`(rootSpan))
        .startSpan()

      val emptyContext = TraceContext.empty
      val contextWithRootSpan = TraceContext(emptyContext.context.`with`(rootSpan))
      val contextWithChildSpan = TraceContext(emptyContext.context.`with`(childSpan))

      val testCases = Seq(emptyContext, contextWithRootSpan, contextWithChildSpan)
      forEvery(testCases) { context =>
        TraceContext.fromProtoV0(context.toProtoV0) shouldBe Right(context)
        TraceContext.fromProtoVersioned(
          context.toProtoVersioned(ProtocolVersion.latestForTest)
        ) shouldBe Right(context)
      }
    }
  }
}
