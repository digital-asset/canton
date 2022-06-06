// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse._
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.topology.{DomainId, Member, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.version.{HasProtoV0, ProtobufVersion, RepresentativeProtocolVersion}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** @param representativeProtocolVersion The representativeProtocolVersion must correspond to the protocol version of
  *                                      every transaction in the list (enforced by the factory method)
  */
sealed abstract case class RegisterTopologyTransactionRequest(
    requestedBy: Member,
    participant: ParticipantId,
    requestId: TopologyRequestId,
    transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    override val domainId: DomainId,
)(val representativeProtocolVersion: RepresentativeProtocolVersion)
    extends ProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with HasProtoV0[v0.RegisterTopologyTransactionRequest] {

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(
      v0.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)
    )

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(
      v1.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)
    )

  override def toProtoV0: v0.RegisterTopologyTransactionRequest =
    v0.RegisterTopologyTransactionRequest(
      requestedBy = requestedBy.toProtoPrimitive,
      participant = participant.uid.toProtoPrimitive,
      requestId = requestId.toProtoPrimitive,
      signedTopologyTransactions = transactions.map(_.getCryptographicEvidence),
      domainId = domainId.unwrap.toProtoPrimitive,
    )
}

object RegisterTopologyTransactionRequest {
  def create(
      requestedBy: Member,
      participant: ParticipantId,
      requestId: TopologyRequestId,
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      domainId: DomainId,
  ): Iterable[RegisterTopologyTransactionRequest] =
    transactions.groupBy(_.representativeProtocolVersion.unwrap).map {
      case (protocolVersion, transactions) =>
        new RegisterTopologyTransactionRequest(
          requestedBy = requestedBy,
          participant = participant,
          requestId = requestId,
          transactions = transactions,
          domainId = domainId,
        )(ProtocolMessage.protocolVersionRepresentativeFor(protocolVersion)) {}
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
    } yield new RegisterTopologyTransactionRequest(
      requestedBy,
      ParticipantId(participantUid),
      requestId,
      transactions,
      DomainId(domainUid),
    )(ProtocolMessage.protocolVersionRepresentativeFor(ProtobufVersion(0))) {}
  }
}
