// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.GenTransactionTreeDeserializationContext
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{SynchronizerLimits, v32}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.TopologyTransactionDeserializationContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

final case class EnvelopeContent(message: UnsignedProtocolMessage)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[EnvelopeContent.type]
) extends HasProtocolVersionedWrapper[EnvelopeContent] {
  @transient override protected lazy val companionObj: EnvelopeContent.type = EnvelopeContent

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
    ProtoVersion(32) -> VersionedProtoCodec(
      ProtocolVersion.v36
    )(v32.EnvelopeContent)(
      supportedProtoVersionPVV(_)(fromProtoV32),
      _.toProtoV32,
    )
  )

  def apply(
      message: UnsignedProtocolMessage,
      protocolVersion: ProtocolVersion,
  ): EnvelopeContent =
    EnvelopeContent(message)(protocolVersionRepresentativeFor(protocolVersion))

  private def fromProtoV32(
      pvv: ProtocolVersionValidation,
      context: (EnvelopeContentDeserializationContext, ProtocolVersion),
      contentP: v32.EnvelopeContent,
  ): ParsingResult[EnvelopeContent] = {
    val (
      EnvelopeContentDeserializationContext(hashOps, synchronizerLimits),
      expectedProtocolVersion,
    ) = context
    val acsContext = AcsCommitmentDeserializationContext(synchronizerLimits)
    import v32.EnvelopeContent.SomeEnvelopeContent as Content
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(32))
      content <- (contentP.someEnvelopeContent match {
        case Content.InformeeMessage(messageP) =>
          val informeeMessageContext =
            GenTransactionTreeDeserializationContext(hashOps, synchronizerLimits)
          InformeeMessage.fromProtoV30(
            pvv,
            (informeeMessageContext, expectedProtocolVersion),
            messageP,
          )
        case Content.EncryptedMultipleViewsMessage(messageP) =>
          EncryptedViewMessage.fromProtoV32(
            pvv,
            messageP,
          )
        case Content.UnassignmentMediatorMessage(messageP) =>
          UnassignmentMediatorMessage.fromProtoV30(
            (hashOps, Source(pvv))
          )(messageP)
        case Content.AssignmentMediatorMessage(messageP) =>
          AssignmentMediatorMessage.fromProtoV30(
            (hashOps, Target(pvv))
          )(messageP)
        case Content.RootHashMessage(messageP) =>
          RootHashMessage.fromProtoV30(
            pvv,
            SerializedRootHashMessagePayload.fromByteString,
          )(messageP)
        case Content.TopologyTransactionsBroadcast(messageP) =>
          TopologyTransactionsBroadcast.fromProtoV30(
            pvv,
            TopologyTransactionDeserializationContext(synchronizerLimits),
            messageP,
          )
        case Content.AcsCommitmentProtocolMessage(messageP) =>
          AcsCommitmentProtocolMessage.fromProtoV32(
            pvv,
            acsContext,
            messageP,
          )
        case Content.LegacyAcsCommitmentProtocolMessage(messageP) =>
          LegacyAcsCommitmentProtocolMessage.fromProtoV30(pvv, messageP)
        case Content.AcsCommitmentSummaryProtocolMessage(messageP) =>
          AcsCommitmentSummaryProtocolMessage.fromProtoV32(pvv, acsContext, messageP)
        case Content.LsuSequencingTestMessage(messageP) =>
          LsuSequencingTestMessage.fromProtoV30(pvv, messageP)
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
    fromByteString(
      EnvelopeContentDeserializationContext(hashOps, synchronizerLimits),
      protocolVersion,
    )(ByteString.copyFrom(bytes)).flatMap(castMessage[M])

  /** [[messageFromByteArray]] without validation.
    *
    * NOTE: Only use this when the input comes from a trusted source, such as the node's own
    * database.
    */
  def messageFromTrustedByteArray[M <: UnsignedProtocolMessage](
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  )(
      bytes: Array[Byte]
  )(implicit cast: ProtocolMessageContentCast[M]): ParsingResult[M] =
    fromTrustedByteString(
      // Without validation, the limits are not read
      (EnvelopeContentDeserializationContext(hashOps, SynchronizerLimits.default), protocolVersion)
    )(ByteString.copyFrom(bytes)).flatMap(castMessage[M])

  private def castMessage[M <: UnsignedProtocolMessage](envelopeContent: EnvelopeContent)(implicit
      cast: ProtocolMessageContentCast[M]
  ): ParsingResult[M] =
    cast
      .toKind(envelopeContent.message)
      .toRight(
        ProtoDeserializationError.OtherError(
          s"Cannot deserialize ${envelopeContent.message} as a ${cast.targetKind}"
        )
      )
}
