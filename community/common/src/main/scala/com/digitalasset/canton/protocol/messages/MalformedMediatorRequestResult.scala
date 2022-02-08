// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.version.VersionedMalformedMediatorRequestResult
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.{RequestId, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{HasProtoV0, HasVersionedWrapper, NoCopy}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.DomainId
import com.google.protobuf.ByteString

/** Sent by the mediator to indicate that a mediator request was malformed.
  * The request counts as being rejected and the request UUID will not be deduplicated.
  *
  * @param requestId The ID of the malformed request
  * @param domainId The domain ID of the mediator
  * @param verdict The rejection reason as a verdict
  */
case class MalformedMediatorRequestResult private (
    override val requestId: RequestId,
    override val domainId: DomainId,
    override val viewType: ViewType,
    override val verdict: Verdict.MediatorReject,
)(val deserializedFrom: Option[ByteString])
    extends MediatorResult
    with SignedProtocolMessageContent
    with HasVersionedWrapper[VersionedMalformedMediatorRequestResult]
    with HasProtoV0[v0.MalformedMediatorRequestResult]
    with NoCopy
    with PrettyPrinting {

  override def hashPurpose: HashPurpose = HashPurpose.MalformedMediatorRequestResult

  override protected[messages] def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.MalformedMediatorRequestResult(
      getCryptographicEvidence
    )

  override protected def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMalformedMediatorRequestResult =
    VersionedMalformedMediatorRequestResult(
      VersionedMalformedMediatorRequestResult.Version.V0(toProtoV0)
    )

  override protected def toProtoV0: v0.MalformedMediatorRequestResult =
    v0.MalformedMediatorRequestResult(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
      rejection = Some(verdict.toProtoMediatorRejectV0),
    )

  override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  override def pretty: Pretty[MalformedMediatorRequestResult] = prettyOfClass(
    param("request id", _.requestId),
    param("reject", _.verdict),
    param("view type", _.viewType),
    param("domain id", _.domainId),
  )
}

object MalformedMediatorRequestResult {
  private def apply(
      requestId: RequestId,
      domainId: DomainId,
      viewType: ViewType,
      verdict: Verdict.MediatorReject,
  )(deserializedFrom: Option[ByteString]): MalformedMediatorRequestResult =
    throw new UnsupportedOperationException("Use the other factory methods instead")

  def apply(
      requestId: RequestId,
      domainId: DomainId,
      viewType: ViewType,
      verdict: Verdict.MediatorReject,
  ): MalformedMediatorRequestResult =
    new MalformedMediatorRequestResult(requestId, domainId, viewType, verdict)(None)

  def fromByteString(
      bytes: ByteString
  ): ParsingResult[MalformedMediatorRequestResult] =
    for {

      protoResultMessage <- ProtoConverter.protoParser(
        VersionedMalformedMediatorRequestResult.parseFrom
      )(bytes)
      result <- fromProtoVersioned(protoResultMessage, bytes)
    } yield result

  private def fromProtoVersioned(
      protoResultMsg: VersionedMalformedMediatorRequestResult,
      bytes: ByteString,
  ): ParsingResult[MalformedMediatorRequestResult] =
    protoResultMsg.version match {
      case VersionedMalformedMediatorRequestResult.Version.Empty =>
        Left(FieldNotSet("VersionedMalformedMediatorRequestResult.version"))
      case VersionedMalformedMediatorRequestResult.Version.V0(resultMsg) =>
        fromProtoV0(resultMsg, bytes)
    }

  private def fromProtoV0(
      protoResultMsg: v0.MalformedMediatorRequestResult,
      bytes: ByteString,
  ): ParsingResult[MalformedMediatorRequestResult] = {

    val v0.MalformedMediatorRequestResult(requestIdP, domainIdP, viewTypeP, rejectP) =
      protoResultMsg
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
        .map(RequestId(_))
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      reject <- ProtoConverter.parseRequired(MediatorReject.fromProtoV0, "rejection", rejectP)
    } yield new MalformedMediatorRequestResult(requestId, domainId, viewType, reject)(Some(bytes))
  }

  implicit val malformedMediatorRequestResultCast
      : SignedMessageContentCast[MalformedMediatorRequestResult] = {
    case m: MalformedMediatorRequestResult => Some(m)
    case _ => None
  }
}
