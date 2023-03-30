// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.ledger.participant.state.v2.SubmitterInfo
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Information about the submitters of the transaction
  */
final case class SubmitterMetadata private (
    actAs: NonEmpty[Set[LfPartyId]],
    applicationId: ApplicationId,
    commandId: CommandId,
    submitterParticipant: ParticipantId,
    salt: Salt,
    submissionId: Option[LedgerSubmissionId],
    dedupPeriod: DeduplicationPeriod,
)(
    hashOps: HashOps,
    val representativeProtocolVersion: RepresentativeProtocolVersion[SubmitterMetadata],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[SubmitterMetadata](hashOps)
    with HasProtocolVersionedWrapper[SubmitterMetadata]
    with ProtocolVersionedMemoizedEvidence {

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override val hashPurpose: HashPurpose = HashPurpose.SubmitterMetadata

  override def pretty: Pretty[SubmitterMetadata] = prettyOfClass(
    param("act as", _.actAs),
    param("application id", _.applicationId),
    param("command id", _.commandId),
    param("submitter participant", _.submitterParticipant),
    param("salt", _.salt),
    paramIfDefined("submission id", _.submissionId),
    param("deduplication period", _.dedupPeriod),
  )

  override def companionObj = SubmitterMetadata

  protected def toProtoV0: v0.SubmitterMetadata = v0.SubmitterMetadata(
    actAs = actAs.toSeq,
    applicationId = applicationId.toProtoPrimitive,
    commandId = commandId.toProtoPrimitive,
    submitterParticipant = submitterParticipant.toProtoPrimitive,
    salt = Some(salt.toProtoV0),
    submissionId = submissionId.getOrElse(""),
    dedupPeriod = Some(SerializableDeduplicationPeriod(dedupPeriod).toProtoV0),
  )

}

object SubmitterMetadata
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      SubmitterMetadata,
      HashOps,
    ] {
  override val name: String = "SubmitterMetadata"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter.mk(ProtocolVersion.v3)(v0.SubmitterMetadata)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def apply(
      actAs: NonEmpty[Set[LfPartyId]],
      applicationId: ApplicationId,
      commandId: CommandId,
      submitterParticipant: ParticipantId,
      salt: Salt,
      submissionId: Option[LedgerSubmissionId],
      dedupPeriod: DeduplicationPeriod,
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): SubmitterMetadata = SubmitterMetadata(
    actAs, // Canton ignores SubmitterInfo.readAs per https://github.com/digital-asset/daml/pull/12136
    applicationId,
    commandId,
    submitterParticipant,
    salt,
    submissionId,
    dedupPeriod,
  )(hashOps, protocolVersionRepresentativeFor(protocolVersion), None)

  def fromSubmitterInfo(hashOps: HashOps)(
      submitterInfo: SubmitterInfo,
      submitterParticipant: ParticipantId,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): Either[String, SubmitterMetadata] = {
    NonEmpty.from(submitterInfo.actAs.toSet).toRight("The actAs set must not be empty.").map {
      actAsNes =>
        SubmitterMetadata(
          actAsNes, // Canton ignores SubmitterInfo.readAs per https://github.com/digital-asset/daml/pull/12136
          ApplicationId(submitterInfo.applicationId),
          CommandId(submitterInfo.commandId),
          submitterParticipant,
          salt,
          submitterInfo.submissionId,
          submitterInfo.deduplicationPeriod,
        )(hashOps, protocolVersionRepresentativeFor(protocolVersion), None)
    }
  }

  private def fromProtoV0(hashOps: HashOps, metaDataP: v0.SubmitterMetadata)(
      bytes: ByteString
  ): ParsingResult[SubmitterMetadata] = {
    val v0.SubmitterMetadata(
      saltOP,
      actAsP,
      applicationIdP,
      commandIdP,
      submitterParticipantP,
      submissionIdP,
      dedupPeriodOP,
    ) = metaDataP
    for {
      submitterParticipant <- ParticipantId
        .fromProtoPrimitive(submitterParticipantP, "SubmitterMetadata.submitter_participant")
      actAs <- actAsP.traverse(
        ProtoConverter
          .parseLfPartyId(_)
          .leftMap(e => ProtoDeserializationError.ValueConversionError("actAs", e.message))
      )
      applicationId <- ApplicationId
        .fromProtoPrimitive(applicationIdP)
        .leftMap(ProtoDeserializationError.ValueConversionError("applicationId", _))
      commandId <- CommandId
        .fromProtoPrimitive(commandIdP)
        .leftMap(ProtoDeserializationError.ValueConversionError("commandId", _))
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", saltOP)
        .leftMap(e => ProtoDeserializationError.ValueConversionError("salt", e.message))
      submissionId <-
        if (submissionIdP.nonEmpty)
          LedgerSubmissionId
            .fromString(submissionIdP)
            .bimap(ProtoDeserializationError.ValueConversionError("submissionId", _), Some(_))
        else Right(None)
      dedupPeriod <- ProtoConverter
        .parseRequired(
          SerializableDeduplicationPeriod.fromProtoV0,
          "SubmitterMetadata.deduplication_period",
          dedupPeriodOP,
        )
        .leftMap(e =>
          ProtoDeserializationError.ValueConversionError("deduplicationPeriod", e.message)
        )

      actAsNes <- NonEmpty
        .from(actAs.toSet)
        .toRight(
          ProtoDeserializationError.ValueConversionError("acsAs", "actAs set must not be empty.")
        )
    } yield SubmitterMetadata(
      actAsNes,
      applicationId,
      commandId,
      submitterParticipant,
      salt,
      submissionId,
      dedupPeriod,
    )(hashOps, protocolVersionRepresentativeFor(ProtoVersion(0)), Some(bytes))
  }
}
