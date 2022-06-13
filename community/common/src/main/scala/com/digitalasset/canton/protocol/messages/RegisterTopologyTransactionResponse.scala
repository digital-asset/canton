// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.topology.{DomainId, Member, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.protocol.{v0, v1}
import com.digitalasset.canton.protocol.v0.RegisterTopologyTransactionResponse.Result.{
  State => ProtoState
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{HasProtoV0, ProtobufVersion, RepresentativeProtocolVersion}
import cats.syntax.traverse._

final case class RegisterTopologyTransactionResponse(
    requestedBy: Member,
    participant: ParticipantId,
    requestId: TopologyRequestId,
    results: Seq[RegisterTopologyTransactionResponse.Result],
    override val domainId: DomainId,
)(val representativeProtocolVersion: RepresentativeProtocolVersion)
    extends ProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with HasProtoV0[v0.RegisterTopologyTransactionResponse]
    with NoCopy {

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(
      v0.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionResponse(toProtoV0)
    )

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(
      v1.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionResponse(toProtoV0)
    )

  override def toProtoV0: v0.RegisterTopologyTransactionResponse =
    v0.RegisterTopologyTransactionResponse(
      requestedBy = requestedBy.toProtoPrimitive,
      participant = participant.uid.toProtoPrimitive,
      requestId.unwrap,
      results.map(_.toProtoV0),
      domainId = domainId.unwrap.toProtoPrimitive,
    )
}

object RegisterTopologyTransactionResponse {
  sealed trait State
  object State {
    @Deprecated
    object Requested extends State

    object Failed extends State

    object Rejected extends State

    object Accepted extends State

    object Duplicate extends State

    /** Unnecessary removes are marked as obsolete */
    object Obsolete extends State

    def isExpectedState(state: State): Boolean = state match {
      case State.Requested => false
      case State.Failed => false
      case State.Rejected => false
      case State.Accepted => true
      case State.Duplicate => true
      case State.Obsolete => true

    }
  }

  // TODO(#9521) uniquePathProtoPrimitive should properly be serialized
  case class Result(uniquePathProtoPrimitive: String, state: State)
      extends HasProtoV0[v0.RegisterTopologyTransactionResponse.Result] {
    override def toProtoV0: v0.RegisterTopologyTransactionResponse.Result = {

      def reply(state: v0.RegisterTopologyTransactionResponse.Result.State) =
        new v0.RegisterTopologyTransactionResponse.Result(
          uniquePath = uniquePathProtoPrimitive,
          state = state,
          errorMessage = "",
        )

      state match {
        case State.Requested => reply(ProtoState.REQUESTED)
        case State.Failed => reply(ProtoState.FAILED)
        case State.Rejected => reply(ProtoState.REJECTED)
        case State.Accepted => reply(ProtoState.ACCEPTED)
        case State.Duplicate => reply(ProtoState.DUPLICATE)
        case State.Obsolete => reply(ProtoState.OBSOLETE)
      }
    }
  }

  object Result {
    def fromProtoV0(result: v0.RegisterTopologyTransactionResponse.Result): ParsingResult[Result] =
      result.state match {
        case ProtoState.MISSING_STATE =>
          Left(
            ProtoDeserializationError.OtherError(
              "Missing state for RegisterTopologyTransactionResponse.State.Result"
            )
          )
        case ProtoState.REQUESTED => Right(Result(result.uniquePath, State.Requested))
        case ProtoState.FAILED => Right(Result(result.uniquePath, State.Failed))
        case ProtoState.REJECTED => Right(Result(result.uniquePath, State.Rejected))
        case ProtoState.ACCEPTED => Right(Result(result.uniquePath, State.Accepted))
        case ProtoState.DUPLICATE => Right(Result(result.uniquePath, State.Duplicate))
        case ProtoState.OBSOLETE => Right(Result(result.uniquePath, State.Obsolete))
        case ProtoState.Unrecognized(unrecognizedValue) =>
          Left(
            ProtoDeserializationError.OtherError(
              s"Unrecognised state for RegisterTopologyTransactionResponse.State.Result: $unrecognizedValue"
            )
          )
      }
  }

  def fromProtoV0(
      message: v0.RegisterTopologyTransactionResponse
  ): ParsingResult[RegisterTopologyTransactionResponse] =
    for {
      requestedBy <- Member.fromProtoPrimitive(message.requestedBy, "requestedBy")
      participantUid <- UniqueIdentifier.fromProtoPrimitive(message.participant, "participant")
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
      requestId <- String255.fromProtoPrimitive(message.requestId, "requestId")
      results <- message.results.traverse(Result.fromProtoV0)
    } yield RegisterTopologyTransactionResponse(
      requestedBy,
      ParticipantId(participantUid),
      requestId,
      results,
      DomainId(domainUid),
    )(ProtocolMessage.protocolVersionRepresentativeFor(ProtobufVersion(0)))
}
