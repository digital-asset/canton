// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.topology.{DomainId, Member, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{HasProtoV0, NoCopy}
import com.digitalasset.canton.version.ProtocolVersion

case class RegisterTopologyTransactionResponse(
    requestedBy: Member,
    participant: ParticipantId,
    requestId: TopologyRequestId,
    results: Seq[v0.RegisterTopologyTransactionResponse.Result],
    override val domainId: DomainId,
) extends ProtocolMessage
    with HasProtoV0[v0.RegisterTopologyTransactionResponse]
    with NoCopy {

  override def toProtoEnvelopeContentV0(version: ProtocolVersion): v0.EnvelopeContent =
    v0.EnvelopeContent(
      v0.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionResponse(toProtoV0)
    )

  override def toProtoV0: v0.RegisterTopologyTransactionResponse =
    v0.RegisterTopologyTransactionResponse(
      requestedBy = requestedBy.toProtoPrimitive,
      participant = participant.uid.toProtoPrimitive,
      requestId.unwrap,
      results,
      domainId = domainId.unwrap.toProtoPrimitive,
    )
}

object RegisterTopologyTransactionResponse {
  def fromProtoV0(
      message: v0.RegisterTopologyTransactionResponse
  ): ParsingResult[RegisterTopologyTransactionResponse] =
    for {
      requestedBy <- Member.fromProtoPrimitive(message.requestedBy, "requestedBy")
      participantUid <- UniqueIdentifier.fromProtoPrimitive(message.participant, "participant")
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
      requestId <- String255.fromProtoPrimitive(message.requestId, "requestId")
    } yield RegisterTopologyTransactionResponse(
      requestedBy,
      ParticipantId(participantUid),
      requestId,
      message.results,
      DomainId(domainUid),
    )
}
