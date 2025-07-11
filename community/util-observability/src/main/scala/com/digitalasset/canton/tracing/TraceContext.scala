// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import cats.Show.Shown
import com.daml.nonempty.NonEmpty
import com.daml.tracing as damlTelemetry
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.daml.lf.data.NoCopy
import io.opentelemetry.api.trace.{Span, Tracer}
import io.opentelemetry.context.Context as OpenTelemetryContext
import io.opentelemetry.sdk.trace.ReadableSpan

import scala.collection.mutable
import scala.language.implicitConversions

/** Container for values tracing operations through canton.
  */
class TraceContext private[tracing] (val context: OpenTelemetryContext)
    extends Equals
    with Serializable
    with NoCopy {

  lazy val asW3CTraceContext: Option[W3CTraceContext] =
    W3CTraceContext.fromOpenTelemetryContext(context)

  private val span: Option[Span] = Option(Span.fromContextOrNull(context))
    .filter(_.getSpanContext.isValid)

  lazy val traceId: Option[String] = span
    .map(_.getSpanContext.getTraceId)

  lazy val spanId: Option[String] = span
    .map(_.getSpanContext.getSpanId)

  lazy val spanParentId: Option[String] = span
    .collect {
      case readableSpan: ReadableSpan if readableSpan.getParentSpanContext.isValid =>
        readableSpan.getParentSpanContext.getSpanId
    }

  lazy val spanName: Option[String] = span
    .collect { case readableSpan: ReadableSpan =>
      readableSpan.getName
    }

  /** Convert to ledger-api server's telemetry context to facilitate integration
    */
  def toDamlTelemetryContext(implicit tracer: Tracer): damlTelemetry.TelemetryContext =
    damlTelemetry.DefaultTelemetryContext(
      tracer,
      Option(Span.fromContextOrNull(context)).getOrElse(Span.getInvalid),
    )

  /** Java serialization method (despite looking unused, Java serialization will use this during our
    * record/replay tests) Delegates to a proxy to do serialization and deserialization. Despite
    * returning a specific type the signature must return `Object` to be picked up by the
    * serialization routines.
    */
  private def writeReplace(): Object =
    new TraceContext.JavaSerializedTraceContext(asW3CTraceContext)

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  override def canEqual(that: Any): Boolean = that.isInstanceOf[TraceContext]
  override def equals(that: Any): Boolean = that match {
    case other: TraceContext =>
      if (this eq other) true
      else other.canEqual(this) && this.asW3CTraceContext == other.asW3CTraceContext
    case _ => false
  }
  override def hashCode(): Int = this.asW3CTraceContext.hashCode

  override def toString: String = {
    val sb = new mutable.StringBuilder()
    sb.append("TraceContext(")
    traceId.foreach(tid => sb.append("trace id=").append(tid))
    asW3CTraceContext.foreach { w3c =>
      if (traceId.nonEmpty) sb.append(", ")
      sb.append("W3C context=").append(w3c.toString)
    }
    sb.append(")")
    sb.result()
  }

  def showTraceId: Shown = Shown(s"tid:${traceId.getOrElse("")}")
}

object TraceContext {
  private[tracing] def apply(context: OpenTelemetryContext): TraceContext = new TraceContext(
    context
  )

  implicit def traceContextFromErrorLoggingContext(
      errorLoggingContext: ErrorLoggingContext
  ): TraceContext = errorLoggingContext.traceContext

  object Implicits {
    object Empty {
      // make the empty trace context available as an implicit
      // typically only useful for tests and blocks where you have no interest in retaining or passing an existing context
      implicit val emptyTraceContext: TraceContext = TraceContext.empty
    }

  }

  val empty: TraceContext = new TraceContext(OpenTelemetryContext.root())

  /** Used for where a trace context should ideally be passed but support has not yet been added. */
  val todo: TraceContext = empty

  /** Run a block with an entirely new TraceContext. */
  def withNewTraceContext[A](name: String)(fn: TraceContext => A): A = {
    val newSpan = NoReportingTracerProvider.tracer.spanBuilder(name).startSpan()
    val openTelemetryContext = newSpan.storeInContext(OpenTelemetryContext.root())
    val newContext = TraceContext(openTelemetryContext)
    val result = fn(newContext)
    newSpan.end()
    result
  }

  def createNew(name: String): TraceContext = withNewTraceContext(name)(identity)

  def wrapWithNewTraceContext[A](name: String)(item: A): Traced[A] =
    withNewTraceContext(name)(implicit traceContext => Traced(item))

  /** Run a block with a TraceContext taken from a Traced wrapper. */
  def withTraceContext[A, B](fn: TraceContext => A => B)(traced: Traced[A]): B =
    fn(traced.traceContext)(traced.value)

  def fromW3CTraceParent(traceParent: String): TraceContext = W3CTraceContext(
    traceParent
  ).toTraceContext

  def withOpenTelemetryContext[A](context: OpenTelemetryContext)(fn: TraceContext => A): A =
    fn(TraceContext(context))

  /** Where we use batching operations create a separate trace-context but mention this in a debug
    * log statement linking it to the trace ids of the contained items. This will allow manual
    * tracing via logs if ever needed. If all non-empty trace contexts in `items` are the same, this
    * trace context will be reused and no log line emitted.
    */
  def ofBatch(
      name: String
  )(items: IterableOnce[HasTraceContext])(logger: TracedLogger): TraceContext = {
    val validTraces = items.iterator.map(_.traceContext).filter(_.traceId.isDefined).toSeq.distinct

    NonEmpty.from(validTraces) match {
      case None =>
        TraceContext.withNewTraceContext(name)(identity) // just generate new trace context
      case Some(validTracesNE) =>
        if (validTracesNE.sizeCompare(1) == 0)
          validTracesNE.head1 // there's only a single trace so stick with that
        else
          withNewTraceContext(name) { implicit traceContext =>
            // log that we're creating a single traceContext from many trace ids
            val traceIds = validTracesNE.map(_.traceId).collect { case Some(traceId) => traceId }
            logger.debug(s"Created batch from traceIds: [${traceIds.mkString(",")}]")
            traceContext
          }
    }
  }

  /** Java serialization and deserialization support for TraceContext */
  private class JavaSerializedTraceContext(w3CTraceContextO: Option[W3CTraceContext])
      extends Serializable {

    /** Java serialization method (not unused - used by record/replay tests). Despite returning a
      * specific type the method must return a Object to be picked up by the Java serialization
      * routines.
      */
    private def readResolve(): Object =
      w3CTraceContextO.map(_.toTraceContext).getOrElse(TraceContext.empty)
  }

  /** Create a trace context from a telemetry context provided by the ledger-api server
    */
  def fromDamlTelemetryContext(telemetryContext: damlTelemetry.TelemetryContext): TraceContext =
    TraceContext(telemetryContext.openTelemetryContext)
}
