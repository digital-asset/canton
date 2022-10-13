// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import cats.Show.Shown
import com.daml.nonempty.NonEmpty
import com.daml.{telemetry as damlTelemetry}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.v0
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
import com.typesafe.scalalogging.Logger
import io.opentelemetry.api.trace.{Span, Tracer}
import io.opentelemetry.context.{Context as OpenTelemetryContext}

import scala.collection.immutable

/** Container for values tracing operations through canton.
  */
class TraceContext private[tracing] (val context: OpenTelemetryContext)
    extends HasVersionedWrapper[VersionedMessage[TraceContext]]
    with Equals
    with Serializable
    with NoCopy
    with PrettyPrinting {

  lazy val asW3CTraceContext: Option[W3CTraceContext] =
    W3CTraceContext.fromOpenTelemetryContext(context)

  /** Expose this trace context into the GRPC context */
  def intoGrpcContext[A](fn: => A): A = TraceContextGrpc.withGrpcContext(this)(fn)

  lazy val traceId: Option[String] = Option(Span.fromContextOrNull(context))
    .filter(_.getSpanContext.isValid)
    .map(_.getSpanContext.getTraceId)

  def toProtoV0: v0.TraceContext = {
    val w3cTraceContext = asW3CTraceContext
    v0.TraceContext(w3cTraceContext.map(_.parent), w3cTraceContext.flatMap(_.state))
  }

  /** Convert to ledger-api server's telemetry context to facilitate integration
    */
  def toDamlTelemetryContext(implicit tracer: Tracer): damlTelemetry.TelemetryContext =
    damlTelemetry.DefaultTelemetryContext(
      tracer,
      Option(Span.fromContextOrNull(context)).getOrElse(Span.getInvalid),
    )

  /** Java serialization method (despite looking unused, Java serialization will use this during our record/replay tests)
    * Delegates to a proxy to do serialization and deserialization.
    * Despite returning a specific type the signature must return `Object` to be picked up by the serialization routines.
    */
  private def writeReplace(): Object =
    new TraceContext.JavaSerializedTraceContext(asW3CTraceContext)

  override def toProtoVersioned(version: ProtocolVersion): VersionedMessage[TraceContext] =
    VersionedMessage(toProtoV0.toByteString, 0)

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  override def canEqual(that: Any): Boolean = that.isInstanceOf[TraceContext]
  override def equals(that: Any): Boolean = that match {
    case other: TraceContext =>
      if (this eq other) true
      else other.canEqual(this) && this.asW3CTraceContext == other.asW3CTraceContext
    case _ => false
  }
  override def hashCode(): Int = this.asW3CTraceContext.hashCode

  override def pretty: Pretty[TraceContext] = prettyOfClass(
    paramIfDefined("trace id", _.traceId.map(_.unquoted)),
    paramIfDefined("W3C context", _.asW3CTraceContext),
  )

  def showTraceId: Shown = Shown(s"tid:${traceId.getOrElse("")}")
}

object TraceContext extends HasVersionedMessageCompanion[TraceContext] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.TraceContext)(fromProtoV0)
  )

  /** The name of the class as used for pretty-printing */
  override protected def name: String = "TraceContext"

  private[tracing] def apply(context: OpenTelemetryContext): TraceContext = new TraceContext(
    context
  )

  object Implicits {
    object Empty {
      // make the empty trace context available as an implicit
      // typically only useful for tests and blocks where you have no interest in retaining or passing an existing context
      implicit val emptyTraceContext: TraceContext = TraceContext.empty
    }

    object Todo {
      implicit val traceContext: TraceContext = TraceContext.todo
    }
  }

  val empty: TraceContext = new TraceContext(OpenTelemetryContext.root())

  /** Used for where a trace context should ideally be passed but support has not yet been added. */
  val todo: TraceContext = empty

  /** Run a block taking a TraceContext which has been constructed from the GRPC context.
    * Typically used to wrap GRPC server methods.
    */
  def fromGrpcContext[A](fn: TraceContext => A): A = fn(TraceContextGrpc.fromGrpcContext)

  /** Run a block with an entirely new TraceContext. */
  def withNewTraceContext[A](fn: TraceContext => A): A = {
    val newSpan = NoReportingTracerProvider.tracer.spanBuilder("newSpan").startSpan()
    val openTelemetryContext = newSpan.storeInContext(OpenTelemetryContext.root())
    val newContext = TraceContext(openTelemetryContext)
    val result = fn(newContext)
    newSpan.end()
    result
  }

  /** Run a block with a TraceContext taken from a Traced wrapper. */
  def withTraceContext[A, B](fn: TraceContext => A => B)(traced: Traced[A]): B =
    fn(traced.traceContext)(traced.value)

  def fromW3CTraceParent(traceParent: String): TraceContext = W3CTraceContext(
    traceParent
  ).toTraceContext

  /** Construct a TraceContext from provided protobuf bytes.
    * Errors will be logged at a WARN level using the provided storageLogger and an empty TraceContext will be returned.
    */
  def fromByteArraySafe(logger: Logger)(bytes: Array[Byte]): TraceContext =
    safely(logger)(fromByteArray)(bytes)

  /** Construct a TraceContext from provided protobuf structure.
    * Errors will be logged at a WARN level using the provided storageLogger and an empty TraceContext will be returned.
    */
  def fromProtoSafeV0Opt(logger: Logger)(traceContextP: Option[v0.TraceContext]): TraceContext =
    safely(logger)(fromProtoV0Opt)(traceContextP)

  def fromProtoV0Opt(
      traceContextP: Option[v0.TraceContext]
  ): ParsingResult[TraceContext] =
    for {
      tcP <- ProtoConverter.required("traceContext", traceContextP)
      tc <- fromProtoV0(tcP)
    } yield tc

  def fromProtoV0(tc: v0.TraceContext): ParsingResult[TraceContext] =
    Right(W3CTraceContext.toTraceContext(tc.traceparent, tc.tracestate))

  /** Where we use batching operations create a separate trace-context but mention this in a debug log statement
    * linking it to the trace ids of the contained items. This will allow manual tracing via logs if ever needed.
    */
  def ofBatch(items: immutable.Iterable[HasTraceContext])(logger: TracedLogger): TraceContext = {
    val validTraces = items.map(_.traceContext).filter(_.traceId.isDefined)

    NonEmpty.from(validTraces) match {
      case None => TraceContext.withNewTraceContext(identity) // just generate new trace context
      case Some(validTracesNE) =>
        if (validTracesNE.sizeCompare(1) == 0)
          validTracesNE.head1 // there's only a single trace so stick with that
        else
          withNewTraceContext { implicit traceContext =>
            // log that we're creating a single traceContext from many trace ids
            val traceIds = validTracesNE.map(_.traceId).collect { case Some(traceId) => traceId }
            logger.info(s"Created batch from traceIds: [${traceIds.mkString(",")}]")
            traceContext
          }
    }
  }

  private def safely[A](
      logger: Logger
  )(fn: A => ParsingResult[TraceContext])(a: A): TraceContext =
    fn(a) match {
      case Left(err) =>
        logger.warn(s"Failed to deserialize provided trace context: $err")
        TraceContext.empty
      case Right(traceContext) => traceContext
    }

  /** Java serialization and deserialization support for TraceContext */
  private class JavaSerializedTraceContext(w3CTraceContextO: Option[W3CTraceContext])
      extends Serializable {

    /** Java serialization method (not unused - used by record/replay tests).
      * Despite returning a specific type the method must return a Object to be picked up by the Java
      * serialization routines.
      */
    private def readResolve(): Object =
      w3CTraceContextO.map(_.toTraceContext).getOrElse(TraceContext.empty)
  }

  /** Create a trace context from a telemetry context provided by the ledger-api server
    */
  def fromDamlTelemetryContext(telemetryContext: damlTelemetry.TelemetryContext): TraceContext =
    TraceContext(telemetryContext.openTelemetryContext)
}
