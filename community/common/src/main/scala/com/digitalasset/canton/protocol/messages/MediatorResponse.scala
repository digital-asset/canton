// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, InvariantViolation}
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.protocol.messages.MediatorResponse.InvalidMediatorResponse
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.version.VersionedMediatorResponse
import com.digitalasset.canton.protocol.{RequestId, RootHash, ViewHash, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{HasProtoV0, HasVersionedWrapper, NoCopy}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.LfPartyId
import com.google.protobuf.ByteString

/** Payload of a response sent to the mediator in reaction to a request.
  *
  * @param requestId The unique identifier of the request.
  * @param sender The identity of the sender.
  * @param viewHash The value of the field "view hash" in the corresponding view message.
  *                 May be empty if the [[localVerdict]] is [[com.digitalasset.canton.protocol.messages.LocalReject.Malformed]].
  * @param localVerdict The participant's verdict on the request's view.
  * @param rootHash The root hash of the request if the local verdict is [[com.digitalasset.canton.protocol.messages.LocalApprove]]
  *                 or [[com.digitalasset.canton.protocol.messages.LocalReject]]. [[scala.None$]] otherwise.
  * @param confirmingParties The non-empty set of confirming parties of the view hosted by the sender if the local verdict is [[com.digitalasset.canton.protocol.messages.LocalApprove]]
  *                          or [[com.digitalasset.canton.protocol.messages.LocalReject]]. Empty otherwise.
  * @param domainId The domain ID over which the request is sent.
  */
case class MediatorResponse private (
    requestId: RequestId,
    sender: ParticipantId,
    viewHash: Option[ViewHash],
    localVerdict: LocalVerdict,
    rootHash: Option[RootHash],
    confirmingParties: Set[LfPartyId],
    override val domainId: DomainId,
)(override val deserializedFrom: Option[ByteString])
    extends SignedProtocolMessageContent
    with HasVersionedWrapper[VersionedMediatorResponse]
    with HasProtoV0[v0.MediatorResponse]
    with HasDomainId
    with NoCopy {

  localVerdict match {
    case _: LocalReject.Malformed =>
      if (confirmingParties.nonEmpty)
        throw InvalidMediatorResponse("Confirming parties must be empty for verdict Malformed.")
    case LocalApprove | _: LocalReject =>
      if (confirmingParties.isEmpty)
        throw InvalidMediatorResponse(
          show"Confirming parties must not be empty for verdict $localVerdict"
        )
      if (rootHash.isEmpty)
        throw InvalidMediatorResponse(show"Root hash must not be empty for verdict $localVerdict")
      if (viewHash.isEmpty)
        throw InvalidMediatorResponse(show"View mash must not be empty for verdict $localVerdict")
  }

  protected override def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  override protected def toProtoVersioned(version: ProtocolVersion): VersionedMediatorResponse =
    VersionedMediatorResponse(VersionedMediatorResponse.Version.V0(toProtoV0))

  override protected def toProtoV0: v0.MediatorResponse =
    v0.MediatorResponse(
      requestId = Some(requestId.unwrap.toProtoPrimitive),
      sender = sender.toProtoPrimitive,
      viewHash = viewHash.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      localVerdict = Some(localVerdict.toProtoV0),
      rootHash = rootHash.fold(ByteString.EMPTY)(_.toProtoPrimitive),
      confirmingParties = confirmingParties.toList,
      domainId = domainId.toProtoPrimitive,
    )

  override def toProtoSomeSignedProtocolMessage
      : v0.SignedProtocolMessage.SomeSignedProtocolMessage.MediatorResponse =
    v0.SignedProtocolMessage.SomeSignedProtocolMessage.MediatorResponse(getCryptographicEvidence)

  override def hashPurpose: HashPurpose = HashPurpose.MediatorResponseSignature
}

object MediatorResponse {

  case class InvalidMediatorResponse(msg: String) extends RuntimeException(msg)

  private[this] def apply(
      requestId: RequestId,
      sender: ParticipantId,
      viewHash: Option[ViewHash],
      localVerdict: LocalVerdict,
      rootHash: Option[RootHash],
      confirmingParties: Set[LfPartyId],
      domainId: DomainId,
  )(deserializedFrom: Option[ByteString]) =
    throw new UnsupportedOperationException("Use the public apply method")

  def create(
      requestId: RequestId,
      sender: ParticipantId,
      viewHash: Option[ViewHash],
      localVerdict: LocalVerdict,
      rootHash: Option[RootHash],
      confirmingParties: Set[LfPartyId],
      domainId: DomainId,
  ): Either[InvalidMediatorResponse, MediatorResponse] =
    Either.catchOnly[InvalidMediatorResponse](
      tryCreate(requestId, sender, viewHash, localVerdict, rootHash, confirmingParties, domainId)
    )

  def tryCreate(
      requestId: RequestId,
      sender: ParticipantId,
      viewHash: Option[ViewHash],
      localVerdict: LocalVerdict,
      rootHash: Option[RootHash],
      confirmingParties: Set[LfPartyId],
      domainId: DomainId,
  ): MediatorResponse =
    new MediatorResponse(
      requestId,
      sender,
      viewHash,
      localVerdict,
      rootHash,
      confirmingParties,
      domainId,
    )(None)

  private def fromProtoV0(
      mediatorResponseP: v0.MediatorResponse,
      bytes: ByteString,
  ): ParsingResult[MediatorResponse] = {
    val v0.MediatorResponse(
      requestIdP,
      senderP,
      viewHashP,
      localVerdictP,
      rootHashP,
      confirmingPartiesP,
      domainIdP,
    ) =
      mediatorResponseP
    for {
      requestId <- ProtoConverter
        .required("MediatorResponse.request_id", requestIdP)
        .flatMap(CantonTimestamp.fromProtoPrimitive)
        .map(RequestId(_))
      sender <- ParticipantId.fromProtoPrimitive(senderP, "MediatorResponse.sender")
      viewHash <- ViewHash.fromProtoPrimitiveOption(viewHashP)
      localVerdict <- ProtoConverter
        .required("MediatorResponse.local_verdict", localVerdictP)
        .flatMap(LocalVerdict.fromProtoV0)
      rootHashO <- RootHash.fromProtoPrimitiveOption(rootHashP)
      confirmingParties <- confirmingPartiesP.traverse(ProtoConverter.parseLfPartyId)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      response <- Either
        .catchOnly[InvalidMediatorResponse](
          new MediatorResponse(
            requestId,
            sender,
            viewHash,
            localVerdict,
            rootHashO,
            confirmingParties.toSet,
            domainId,
          )(Some(bytes))
        )
        .leftMap(err => InvariantViolation(err.toString))
    } yield response
  }

  private def fromProtoVersioned(
      responseP: VersionedMediatorResponse,
      bytes: ByteString,
  ): ParsingResult[MediatorResponse] =
    responseP.version match {
      case VersionedMediatorResponse.Version.Empty =>
        Left(FieldNotSet("VersionedMediatorResponse.version"))
      case VersionedMediatorResponse.Version.V0(response) => fromProtoV0(response, bytes)
    }

  def fromByteString(bytes: ByteString): ParsingResult[MediatorResponse] = {
    for {
      mediatorResponse <- ProtoConverter.protoParser(VersionedMediatorResponse.parseFrom)(bytes)
      response <- fromProtoVersioned(mediatorResponse, bytes)
    } yield response
  }

  implicit val mediatorResponseSignedMessageContentCast
      : SignedMessageContentCast[MediatorResponse] = {
    case response: MediatorResponse => Some(response)
    case _ => None
  }
}
