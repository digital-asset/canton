// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.{RequestId, v0, v1}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWrapperCompanion,
  HasProtocolVersionedWrapper,
  ProtobufVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

/** Sent by the mediator to indicate that a mediator request was malformed.
  * The request counts as being rejected and the request UUID will not be deduplicated.
  *
  * @param requestId The ID of the malformed request
  * @param domainId The domain ID of the mediator
  * @param verdict The rejection reason as a verdict
  */
sealed abstract case class MalformedMediatorRequestResult(
    override val requestId: RequestId,
    override val domainId: DomainId,
    override val viewType: ViewType,
    override val verdict: Verdict.MediatorReject,
)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[
      MalformedMediatorRequestResult
    ],
    val deserializedFrom: Option[ByteString],
) extends MediatorResult
    with SignedProtocolMessageContent
    with HasProtocolVersionedWrapper[MalformedMediatorRequestResult]
    with NoCopy
    with PrettyPrinting {

  override def hashPurpose: HashPurpose = HashPurpose.MalformedMediatorRequestResult

  override protected[messages] def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.MalformedMediatorRequestResult(
      getCryptographicEvidence
    )

  override def companionObj = MalformedMediatorRequestResult

  protected def toProtoV0: v0.MalformedMediatorRequestResult =
    v0.MalformedMediatorRequestResult(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
      rejection = Some(verdict.toProtoMediatorRejectionV0),
    )

  protected def toProtoV1: v1.MalformedMediatorRequestResult =
    v1.MalformedMediatorRequestResult(
      requestId = Some(requestId.toProtoPrimitive),
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
      rejection = Some(verdict.toProtoMediatorRejectV1),
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[MalformedMediatorRequestResult] = prettyOfClass(
    param("request id", _.requestId),
    param("reject", _.verdict),
    param("view type", _.viewType),
    param("domain id", _.domainId),
  )
}

object MalformedMediatorRequestResult
    extends HasMemoizedProtocolVersionedWrapperCompanion[MalformedMediatorRequestResult] {
  override val name: String = "MalformedMediatorRequestResult"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtobufVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersionMemoized(v0.MalformedMediatorRequestResult)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtobufVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.dev, // TODO(i10131): make stable
      supportedProtoVersionMemoized(v1.MalformedMediatorRequestResult)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def apply(
      requestId: RequestId,
      domainId: DomainId,
      viewType: ViewType,
      verdict: Verdict.MediatorReject,
      protocolVersion: ProtocolVersion,
  ): MalformedMediatorRequestResult =
    new MalformedMediatorRequestResult(requestId, domainId, viewType, verdict)(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    ) {}

  private def fromProtoV0(protoResultMsg: v0.MalformedMediatorRequestResult)(
      bytes: ByteString
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
    } yield new MalformedMediatorRequestResult(requestId, domainId, viewType, reject)(
      protocolVersionRepresentativeFor(ProtobufVersion(0)),
      Some(bytes),
    ) {}
  }

  private def fromProtoV1(malformedMediatorRequestResultP: v1.MalformedMediatorRequestResult)(
      bytes: ByteString
  ): ParsingResult[MalformedMediatorRequestResult] = {

    val v1.MalformedMediatorRequestResult(requestIdPO, domainIdP, viewTypeP, rejectionPO) =
      malformedMediatorRequestResultP
    for {
      requestId <- ProtoConverter
        .required("request_id", requestIdPO)
        .flatMap(RequestId.fromProtoPrimitive)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      reject <- ProtoConverter.parseRequired(MediatorReject.fromProtoV1, "rejection", rejectionPO)
    } yield new MalformedMediatorRequestResult(requestId, domainId, viewType, reject)(
      protocolVersionRepresentativeFor(ProtobufVersion(1)),
      Some(bytes),
    ) {}
  }

  implicit val malformedMediatorRequestResultCast
      : SignedMessageContentCast[MalformedMediatorRequestResult] = {
    case m: MalformedMediatorRequestResult => Some(m)
    case _ => None
  }
}
