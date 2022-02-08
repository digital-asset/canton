// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.tracing.TestTelemetry.eventsOrderedByTime
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.trace.{StatusCode, Tracer}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
@nowarn("msg=match may not be exhaustive")
class SpanningTest extends AnyWordSpec with BaseTest with BeforeAndAfterEach {
  private val exception = new RuntimeException("exception thrown")

  var testTelemetrySetup: TestTelemetrySetup = _

  override def beforeEach(): Unit = {
    testTelemetrySetup = new TestTelemetrySetup()
  }

  override def afterEach(): Unit = {
    testTelemetrySetup.close()
  }

  private class Inner(implicit tracer: Tracer) extends Spanning {
    def foo()(implicit traceContext: TraceContext): Unit = withSpan("Inner.foo") { _ => span =>
      span.addEvent("running Inner.foo")
      span.addEvent("finished Inner.foo")
    }
  }

  private class Outer(inner: Inner)(implicit tracer: Tracer) extends Spanning {
    def foo(): Unit = withNewTrace("Outer.foo") { implicit traceContext => span =>
      span.addEvent("running Outer.foo")
      inner.foo()
      span.addEvent("finished Outer.foo")
    }

    @nowarn("cat=unused")
    def exceptionally()(implicit traceContext: TraceContext): Unit = withSpan("exceptionally") {
      implicit traceContext => _ =>
        throw exception
    }
  }

  "objects with span reporting" should {
    "report root and child spans with events" in {
      implicit val tracer: Tracer = testTelemetrySetup.tracer
      val sut = new Outer(new Inner())

      sut.foo()

      val List(rootSpan, childSpan) = testTelemetrySetup.reportedSpans()
      childSpan.getParentSpanId shouldBe rootSpan.getSpanId
      rootSpan.getName shouldBe "Outer.foo"
      childSpan.getName shouldBe "Inner.foo"
      rootSpan.getTraceId shouldBe childSpan.getTraceId
      eventsOrderedByTime(rootSpan, childSpan).map(_.getName) should contain.inOrderOnly(
        "running Outer.foo",
        "running Inner.foo",
        "finished Inner.foo",
        "finished Outer.foo",
      )
    }

    "report exception and re-throw it" in {
      implicit val tracer: Tracer = testTelemetrySetup.tracer
      val sut = new Outer(new Inner())

      a[RuntimeException] should be thrownBy sut.exceptionally()

      val List(span) = testTelemetrySetup.reportedSpans()

      span.getStatus.getStatusCode shouldBe StatusCode.ERROR
      span.getName shouldBe "exceptionally"

      val List(exceptionEvent) = eventsOrderedByTime(span)
      exceptionEvent.getName shouldBe "exception"
      exceptionEvent.getAttributes.get(stringKey("exception.message")) shouldBe exception.getMessage
      exceptionEvent.getAttributes.get(
        stringKey("exception.type")
      ) shouldBe exception.getClass.getName
    }
  }
}
