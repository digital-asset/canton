// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse._
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.topology.{DomainId, Member, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.util.HasProtoV0
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

case class RegisterTopologyTransactionRequest(
    requestedBy: Member,
    participant: ParticipantId,
    requestId: TopologyRequestId,
    transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    override val domainId: DomainId,
) extends ProtocolMessage
    with HasProtoV0[v0.RegisterTopologyTransactionRequest] {

  override def toProtoEnvelopeContentV0(version: ProtocolVersion): v0.EnvelopeContent =
    v0.EnvelopeContent(
      v0.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)
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
    )
  }
}
