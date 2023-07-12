// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v0, v1, v2, v3}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{DomainId, Member, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

/** @param representativeProtocolVersion The representativeProtocolVersion must correspond to the protocol version of
  *                                      every transaction in the list (enforced by the factory method)
  */
final case class RegisterTopologyTransactionRequest private (
    requestedBy: Member,
    participant: ParticipantId,
    requestId: TopologyRequestId,
    transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      RegisterTopologyTransactionRequest.type
    ]
) extends UnsignedProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with ProtocolMessageV2
    with UnsignedProtocolMessageV3
    with PrettyPrinting {

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(
      v0.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)
    )

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(
      v1.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)
    )

  override def toProtoEnvelopeContentV2: v2.EnvelopeContent =
    v2.EnvelopeContent(
      v2.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)
    )

  override def toProtoSomeEnvelopeContentV3: v3.EnvelopeContent.SomeEnvelopeContent =
    v3.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)

  def toProtoV0: v0.RegisterTopologyTransactionRequest =
    v0.RegisterTopologyTransactionRequest(
      requestedBy = requestedBy.toProtoPrimitive,
      participant = participant.uid.toProtoPrimitive,
      requestId = requestId.toProtoPrimitive,
      signedTopologyTransactions = transactions.map(_.getCryptographicEvidence),
      domainId = domainId.unwrap.toProtoPrimitive,
    )

  @transient override protected lazy val companionObj: RegisterTopologyTransactionRequest.type =
    RegisterTopologyTransactionRequest

  override def pretty: Pretty[RegisterTopologyTransactionRequest] = prettyOfClass(
    param("requestBy", _.requestedBy),
    param("participant", _.participant),
    param("requestId", _.requestId.unwrap.doubleQuoted),
    param("numTx", _.transactions.length),
  )

}

object RegisterTopologyTransactionRequest
    extends HasProtocolVersionedCompanion[RegisterTopologyTransactionRequest] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(
      v0.RegisterTopologyTransactionRequest
    )(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def create(
      requestedBy: Member,
      participant: ParticipantId,
      requestId: TopologyRequestId,
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): Iterable[RegisterTopologyTransactionRequest] = {
    // TODO(#11255) this isn't a good idea. txs shouldn't be regrouped. agreed with raf: we should remove this
    //   the topology txs are serialized as bytestrings individually, which means the version differences don't matter
    transactions.groupBy(_.representativeProtocolVersion).map {
      case (_transactionRepresentativeProtocolVersion, transactions) =>
        RegisterTopologyTransactionRequest(
          requestedBy = requestedBy,
          participant = participant,
          requestId = requestId,
          transactions = transactions,
          domainId = domainId,
        )(protocolVersionRepresentativeFor(protocolVersion))
    }
  }

  def fromProtoV0(
      message: v0.RegisterTopologyTransactionRequest
  ): ParsingResult[RegisterTopologyTransactionRequest] = {
    for {
      requestedBy <- Member.fromProtoPrimitive(message.requestedBy, "requestedBy")
      participantUid <- UniqueIdentifier.fromProtoPrimitive(message.participant, "participant")
      transactions <- message.signedTopologyTransactions.toList.traverse(elem =>
        SignedTopologyTransaction.fromByteString(elem)
      )
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
      requestId <- String255.fromProtoPrimitive(message.requestId, "requestId")
    } yield RegisterTopologyTransactionRequest(
      requestedBy,
      ParticipantId(participantUid),
      requestId,
      transactions,
      DomainId(domainUid),
    )(protocolVersionRepresentativeFor(ProtoVersion(0)))
  }

  override protected def name: String = "RegisterTopologyTransactionRequest"

  implicit val registerTopologyTransactionRequestCast
      : ProtocolMessageContentCast[RegisterTopologyTransactionRequest] =
    ProtocolMessageContentCast.create[RegisterTopologyTransactionRequest](
      "RegisterTopologyTransactionRequest"
    ) {
      case rttr: RegisterTopologyTransactionRequest => Some(rttr)
      case _ => None
    }
}

final case class RegisterTopologyTransactionRequestX private (
    requestedBy: Member,
    requestedFor: Member,
    requestId: TopologyRequestId,
    transactions: List[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      RegisterTopologyTransactionRequestX.type
    ]
) extends UnsignedProtocolMessage
    with UnsignedProtocolMessageV3
    with PrettyPrinting {
  @transient override protected lazy val companionObj: RegisterTopologyTransactionRequestX.type =
    RegisterTopologyTransactionRequestX

  def toProtoV2: v2.RegisterTopologyTransactionRequestX =
    v2.RegisterTopologyTransactionRequestX(
      requestedBy = requestedBy.toProtoPrimitive,
      requestedFor = requestedFor.toProtoPrimitive,
      requestId = requestId.toProtoPrimitive,
      transactions = transactions.map(_.toProtoV2),
      domain = domainId.toProtoPrimitive,
    )

  override protected[messages] def toProtoSomeEnvelopeContentV3
      : v3.EnvelopeContent.SomeEnvelopeContent =
    v3.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequestX(toProtoV2)

  override def pretty: Pretty[RegisterTopologyTransactionRequestX] = prettyOfClass(
    param("requestBy", _.requestedBy),
    param("requestedFor", _.requestedFor),
    param("requestId", _.requestId.unwrap.doubleQuoted),
    param("numTx", _.transactions.length),
  )

}

object RegisterTopologyTransactionRequestX
    extends HasProtocolVersionedCompanion[RegisterTopologyTransactionRequestX] {

  override protected def name: String = "RegisterTopologyTransactionRequestX"

  def create(
      requestedBy: Member,
      requestedFor: Member,
      requestId: TopologyRequestId,
      transactions: List[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): RegisterTopologyTransactionRequestX = RegisterTopologyTransactionRequestX(
    requestedBy,
    requestedFor,
    requestId,
    transactions,
    domainId,
  )((RegisterTopologyTransactionRequestX.protocolVersionRepresentativeFor(protocolVersion)))

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(-1) -> UnsupportedProtoCodec(ProtocolVersion.minimum),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.dev)(
      v2.RegisterTopologyTransactionRequestX
    )(
      supportedProtoVersion(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
  )

  def fromProtoV2(
      message: v2.RegisterTopologyTransactionRequestX
  ): ParsingResult[RegisterTopologyTransactionRequestX] = {
    val v2.RegisterTopologyTransactionRequestX(
      requestedBy,
      requestedFor,
      requestId,
      transactions,
      domain,
    ) = message
    for {
      requestedBy <- Member.fromProtoPrimitive(requestedBy, "requested_by")
      requestedFor <- Member.fromProtoPrimitive(requestedFor, "requested_for")
      requestId <- String255.fromProtoPrimitive(requestId, "request_id")
      transactions <- transactions.toList.traverse(SignedTopologyTransactionX.fromProtoV2)
      domainId <- DomainId.fromProtoPrimitive(domain, "domain")
    } yield RegisterTopologyTransactionRequestX(
      requestedBy,
      requestedFor,
      requestId,
      transactions,
      domainId,
    )(protocolVersionRepresentativeFor(ProtoVersion(2)))
  }

}
