// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party.acsreplication

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

final case class AcsReplicationTargetParticipantMessage(
    instruction: AcsReplicationTargetParticipantMessage.Instruction
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      AcsReplicationTargetParticipantMessage.type
    ]
) extends HasProtocolVersionedWrapper[AcsReplicationTargetParticipantMessage] {
  @transient override protected lazy val companionObj: AcsReplicationTargetParticipantMessage.type =
    AcsReplicationTargetParticipantMessage

  def toProtoV30: v30.AcsReplicationTargetParticipantMessage =
    v30.AcsReplicationTargetParticipantMessage(instruction.toProtoV30)
}

object AcsReplicationTargetParticipantMessage
    extends VersioningCompanion[AcsReplicationTargetParticipantMessage] {

  override val name: String = "PartyReplicationTargetParticipantMessage"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.dev)(
      v30.AcsReplicationTargetParticipantMessage
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def fromProtoV30(
      proto: v30.AcsReplicationTargetParticipantMessage
  ): ParsingResult[AcsReplicationTargetParticipantMessage] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    instruction <- proto.instruction match {
      case v30.AcsReplicationTargetParticipantMessage.Instruction.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("instruction"))
      case v30.AcsReplicationTargetParticipantMessage.Instruction.Initialize(
            v30.AcsReplicationTargetParticipantMessage.Initialize(
              initialContractOrdinalInclusive
            )
          ) =>
        ProtoConverter
          .parseNonNegativeLong(
            "initial_contract_ordinal_inclusive",
            initialContractOrdinalInclusive,
          )
          .map(Initialize.apply)
      case v30.AcsReplicationTargetParticipantMessage.Instruction.SendAcsUpTo(
            v30.AcsReplicationTargetParticipantMessage.SendAcsUpTo(
              maxContractOrdinalInclusive
            )
          ) =>
        ProtoConverter
          .parseNonNegativeLong(
            "max_contract_ordinal_inclusive",
            maxContractOrdinalInclusive,
          )
          .map[Instruction](SendAcsUpTo.apply)
    }
  } yield AcsReplicationTargetParticipantMessage(instruction)(rpv)

  sealed trait Instruction {
    def toProtoV30: v30.AcsReplicationTargetParticipantMessage.Instruction
  }

  final case class Initialize(initialContractOrdinalInclusive: NonNegativeLong)
      extends Instruction {
    override def toProtoV30: v30.AcsReplicationTargetParticipantMessage.Instruction =
      v30.AcsReplicationTargetParticipantMessage.Instruction.Initialize(
        v30.AcsReplicationTargetParticipantMessage
          .Initialize(initialContractOrdinalInclusive.value)
      )
  }

  final case class SendAcsUpTo(maxContractOrdinalInclusive: NonNegativeLong) extends Instruction {
    override def toProtoV30: v30.AcsReplicationTargetParticipantMessage.Instruction =
      v30.AcsReplicationTargetParticipantMessage.Instruction.SendAcsUpTo(
        v30.AcsReplicationTargetParticipantMessage
          .SendAcsUpTo(maxContractOrdinalInclusive.value)
      )
  }

  def apply(
      instruction: AcsReplicationTargetParticipantMessage.Instruction,
      protocolVersion: ProtocolVersion,
  ): AcsReplicationTargetParticipantMessage =
    AcsReplicationTargetParticipantMessage(instruction)(
      protocolVersionRepresentativeFor(protocolVersion)
    )
}
