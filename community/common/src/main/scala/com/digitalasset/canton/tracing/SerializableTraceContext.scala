// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.v0
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionCommon,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.typesafe.scalalogging.Logger

/** Wrapper around [[TraceContext]] to keep serialization out of the [[TraceContext]] itself
  * and thereby reduce its dependencies.
  */
case class SerializableTraceContext(traceContext: TraceContext)
    extends HasVersionedWrapper[SerializableTraceContext] {

  def unwrap: TraceContext = traceContext

  override protected def companionObj
      : HasVersionedMessageCompanionCommon[SerializableTraceContext] = SerializableTraceContext

  def toProtoV0: v0.TraceContext = {
    val w3cTraceContext = traceContext.asW3CTraceContext
    v0.TraceContext(w3cTraceContext.map(_.parent), w3cTraceContext.flatMap(_.state))
  }
}

object SerializableTraceContext
    extends HasVersionedMessageCompanion[SerializableTraceContext]
    with HasVersionedMessageCompanionDbHelpers[SerializableTraceContext] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.TraceContext)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  /** The name of the class as used for pretty-printing */
  override protected def name: String = "TraceContext"

  val empty: SerializableTraceContext = SerializableTraceContext(TraceContext.empty)

  /** Construct a TraceContext from provided protobuf bytes.
    * Errors will be logged at a WARN level using the provided storageLogger and an empty TraceContext will be returned.
    */
  def fromByteArraySafe(logger: Logger)(bytes: Array[Byte]): SerializableTraceContext =
    safely(logger)(fromByteArray)(bytes)

  /** Construct a TraceContext from provided protobuf structure.
    * Errors will be logged at a WARN level using the provided storageLogger and an empty TraceContext will be returned.
    */
  def fromProtoSafeV0Opt(logger: Logger)(
      traceContextP: Option[v0.TraceContext]
  ): SerializableTraceContext =
    safely(logger)(fromProtoV0Opt)(traceContextP)

  def fromProtoV0Opt(
      traceContextP: Option[v0.TraceContext]
  ): ParsingResult[SerializableTraceContext] =
    for {
      tcP <- ProtoConverter.required("traceContext", traceContextP)
      tc <- fromProtoV0(tcP)
    } yield tc

  def fromProtoV0(tc: v0.TraceContext): ParsingResult[SerializableTraceContext] =
    Right(SerializableTraceContext(W3CTraceContext.toTraceContext(tc.traceparent, tc.tracestate)))

  private def safely[A](
      logger: Logger
  )(fn: A => ParsingResult[SerializableTraceContext])(a: A): SerializableTraceContext =
    fn(a) match {
      case Left(err) =>
        logger.warn(s"Failed to deserialize provided trace context: $err")
        SerializableTraceContext(TraceContext.empty)
      case Right(traceContext) => traceContext
    }
}
