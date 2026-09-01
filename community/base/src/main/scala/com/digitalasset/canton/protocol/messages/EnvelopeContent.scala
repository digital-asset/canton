// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.GenTransactionTreeDeserializationContext
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{SynchronizerLimits, v30, v31, v32}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

final case class EnvelopeContent(message: UnsignedProtocolMessage)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent.type]
) extends HasProtocolVersionedWrapper[EnvelopeContent] {
  @transient override protected lazy val companionObj: EnvelopeContent.type = EnvelopeContent

  private def toProtoV30: v30.EnvelopeContent =
    v30.EnvelopeContent(message.toProtoSomeEnvelopeContentV30)

  private def toProtoV31: v31.EnvelopeContent =
    v31.EnvelopeContent(message.toProtoSomeEnvelopeContentV31)

  private def toProtoV32: v32.EnvelopeContent =
    v32.EnvelopeContent(message.toProtoSomeEnvelopeContentV32)
}

final case class EnvelopeContentDeserializationContext(
    hashOps: HashOps,
    synchronizerLimits: SynchronizerLimits,
)

object EnvelopeContent
    extends VersioningCompanionContextPVValidation2[
      EnvelopeContent,
      EnvelopeContentDeserializationContext,
    ] {

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(
      ProtocolVersion.v34
    )(v30.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
    ProtoVersion(31) -> VersionedProtoCodec(
      ProtocolVersion.v35
    )(v31.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV31),
      _.toProtoV31,
    ),
    ProtoVersion(32) -> VersionedProtoCodec(
      ProtocolVersion.v36
    )(v32.EnvelopeContent)(
      supportedProtoVersion(_)(fromProtoV32),
      _.toProtoV32,
    ),
  )

  def apply(
      message: UnsignedProtocolMessage,
      protocolVersion: ProtocolVersion,
  ): EnvelopeContent =
    EnvelopeContent(message)(protocolVersionRepresentativeFor(protocolVersion))

  private def fromProtoV30(
      context: (EnvelopeContentDeserializationContext, ProtocolVersion),
      contentP: v30.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    val (
      EnvelopeContentDeserializationContext(hashOps, synchronizerLimits),
      expectedProtocolVersion,
    ) = context
    import v30.EnvelopeContent.SomeEnvelopeContent as Content
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      content <- (contentP.someEnvelopeContent match {
        case Content.InformeeMessage(messageP) =>
          val informeeMessageContext =
            GenTransactionTreeDeserializationContext(hashOps, synchronizerLimits)
          InformeeMessage.fromProtoV30((informeeMessageContext, expectedProtocolVersion))(messageP)
        case Content.EncryptedViewMessage(messageP) =>
          EncryptedSingleViewMessage.fromProto(
            ProtocolVersionValidation.PV(expectedProtocolVersion),
            messageP,
          )
        case Content.UnassignmentMediatorMessage(messageP) =>
          UnassignmentMediatorMessage.fromProtoV30(
            (hashOps, Source(ProtocolVersionValidation.PV(expectedProtocolVersion)))
          )(messageP)
        case Content.AssignmentMediatorMessage(messageP) =>
          AssignmentMediatorMessage.fromProtoV30(
            (hashOps, Target(expectedProtocolVersion))
          )(messageP)
        case Content.RootHashMessage(messageP) =>
          RootHashMessage.fromProtoV30(
            ProtocolVersionValidation.PV(expectedProtocolVersion),
            SerializedRootHashMessagePayload.fromByteString,
          )(messageP)
        case Content.TopologyTransactionsBroadcast(messageP) =>
          TopologyTransactionsBroadcast.fromProtoV30(expectedProtocolVersion, messageP)
        case Content.LsuSequencingTestMessage(messageP) =>
          LsuSequencingTestMessage.fromProtoV30(expectedProtocolVersion, messageP)
        case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
      }): ParsingResult[UnsignedProtocolMessage]
    } yield EnvelopeContent(content)(rpv)
  }

  private def fromProtoV31(
      context: (EnvelopeContentDeserializationContext, ProtocolVersion),
      contentP: v31.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    val (
      EnvelopeContentDeserializationContext(hashOps, synchronizerLimits),
      expectedProtocolVersion,
    ) = context
    import v31.EnvelopeContent.SomeEnvelopeContent as Content
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))
      content <- (contentP.someEnvelopeContent match {
        case Content.InformeeMessage(messageP) =>
          val informeeMessageContext =
            GenTransactionTreeDeserializationContext(hashOps, synchronizerLimits)
          InformeeMessage.fromProtoV30((informeeMessageContext, expectedProtocolVersion))(messageP)
        case Content.EncryptedMultipleViewsMessage(messageP) =>
          EncryptedMultipleViewsMessage.fromProtoV31(
            ProtocolVersionValidation.PV(expectedProtocolVersion),
            messageP,
          )
        case Content.UnassignmentMediatorMessage(messageP) =>
          UnassignmentMediatorMessage.fromProtoV30(
            (hashOps, Source(ProtocolVersionValidation.PV(expectedProtocolVersion)))
          )(messageP)
        case Content.AssignmentMediatorMessage(messageP) =>
          AssignmentMediatorMessage.fromProtoV30(
            (hashOps, Target(expectedProtocolVersion))
          )(messageP)
        case Content.RootHashMessage(messageP) =>
          RootHashMessage.fromProtoV30(
            ProtocolVersionValidation.PV(expectedProtocolVersion),
            SerializedRootHashMessagePayload.fromByteString,
          )(messageP)
        case Content.TopologyTransactionsBroadcast(messageP) =>
          TopologyTransactionsBroadcast.fromProtoV30(expectedProtocolVersion, messageP)
        case Content.AcsCommitmentProtocolMessage(messageP) =>
          LegacyAcsCommitmentProtocolMessage.fromProtoV30(expectedProtocolVersion, messageP)
        case Content.LsuSequencingTestMessage(messageP) =>
          LsuSequencingTestMessage.fromProtoV30(expectedProtocolVersion, messageP)
        case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
      }): ParsingResult[UnsignedProtocolMessage]
    } yield EnvelopeContent(content)(rpv)
  }

  private def fromProtoV32(
      context: (EnvelopeContentDeserializationContext, ProtocolVersion),
      contentP: v32.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    val (
      EnvelopeContentDeserializationContext(hashOps, synchronizerLimits),
      expectedProtocolVersion,
    ) = context
    import v32.EnvelopeContent.SomeEnvelopeContent as Content
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(32))
      content <- (contentP.someEnvelopeContent match {
        case Content.InformeeMessage(messageP) =>
          val informeeMessageContext =
            GenTransactionTreeDeserializationContext(hashOps, synchronizerLimits)
          InformeeMessage.fromProtoV30((informeeMessageContext, expectedProtocolVersion))(messageP)
        case Content.EncryptedMultipleViewsMessage(messageP) =>
          EncryptedMultipleViewsMessage.fromProtoV32(
            ProtocolVersionValidation.PV(expectedProtocolVersion),
            messageP,
          )
        case Content.UnassignmentMediatorMessage(messageP) =>
          UnassignmentMediatorMessage.fromProtoV30(
            (hashOps, Source(ProtocolVersionValidation.PV(expectedProtocolVersion)))
          )(messageP)
        case Content.AssignmentMediatorMessage(messageP) =>
          AssignmentMediatorMessage.fromProtoV30(
            (hashOps, Target(expectedProtocolVersion))
          )(messageP)
        case Content.RootHashMessage(messageP) =>
          RootHashMessage.fromProtoV30(
            ProtocolVersionValidation.PV(expectedProtocolVersion),
            SerializedRootHashMessagePayload.fromByteString,
          )(messageP)
        case Content.TopologyTransactionsBroadcast(messageP) =>
          TopologyTransactionsBroadcast.fromProtoV30(expectedProtocolVersion, messageP)
        case Content.AcsCommitmentProtocolMessage(messageP) =>
          AcsCommitmentProtocolMessage.fromProtoV32(
            ProtocolVersionValidation.PV(expectedProtocolVersion),
            messageP,
          )
        case Content.LegacyAcsCommitmentProtocolMessage(messageP) =>
          LegacyAcsCommitmentProtocolMessage.fromProtoV30(expectedProtocolVersion, messageP)
        case Content.AcsCommitmentSummaryProtocolMessage(messageP) =>
          AcsCommitmentSummaryProtocolMessage.fromProtoV32(expectedProtocolVersion, messageP)
        case Content.LsuSequencingTestMessage(messageP) =>
          LsuSequencingTestMessage.fromProtoV30(expectedProtocolVersion, messageP)
        case Content.Empty => Left(OtherError("Cannot deserialize an empty message content"))
      }): ParsingResult[UnsignedProtocolMessage]
    } yield EnvelopeContent(content)(rpv)
  }

  override def name: String = "EnvelopeContent"

  def messageFromByteArray[M <: UnsignedProtocolMessage](
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
      synchronizerLimits: SynchronizerLimits,
  )(
      bytes: Array[Byte]
  )(implicit cast: ProtocolMessageContentCast[M]): ParsingResult[M] =
    for {
      envelopeContent <- fromByteString(
        EnvelopeContentDeserializationContext(hashOps, synchronizerLimits),
        protocolVersion,
      )(
        ByteString.copyFrom(bytes)
      )
      message <- cast
        .toKind(envelopeContent.message)
        .toRight(
          ProtoDeserializationError.OtherError(
            s"Cannot deserialize ${envelopeContent.message} as a ${cast.targetKind}"
          )
        )
    } yield message
}
