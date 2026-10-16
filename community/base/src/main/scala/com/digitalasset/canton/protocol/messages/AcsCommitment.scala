// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.serialization.ProtoConverter.{
  ParsingResult,
  parseLfParticipantId,
  parseRequired,
}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.validation.{ProtoValidation, ValidationParam}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionMemoization,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

final case class AcsCommitment private (
    psid: PhysicalSynchronizerId,
    sender: LedgerParticipantId,
    counterparticipant: LedgerParticipantId,
    period: CommitmentPeriod,
    digest: Digest.DigestType,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[AcsCommitment.type],
    override val deserializedFrom: Option[ByteString],
) extends HasProtocolVersionedWrapper[AcsCommitment]
    with ProtocolVersionedMemoizedEvidence
    with PrettyPrintingFromCompanion
    with NoCopy {

  @transient override protected lazy val companionObj: AcsCommitment.type = AcsCommitment

  private def toProtoV32: v32.AcsCommitment = v32.AcsCommitment(
    physicalSynchronizerId = psid.toProtoPrimitive,
    sendingParticipantUid = sender,
    counterparticipantUid = counterparticipant,
    period = period.toProtoV32.some,
    digest = digest,
  )

  override def prettyCompanion: PrettyPrintingCompanion[AcsCommitment] = AcsCommitment

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString
}

object AcsCommitment
    extends VersioningCompanionMemoization[AcsCommitment]
    with PrettyPrintingCompanion[AcsCommitment] {
  override val name: String = "AcsCommitment"

  override protected val pretty: Pretty[AcsCommitment] =
    prettyOfClass(
      param("psid", _.psid),
      param("sender", _.sender),
      param("counterparticipant", _.counterparticipant),
      param("period", _.period),
      param("commitment", _.digest),
    )

  // Size upper bound for SHA-256 hash (32 byte digest + 2 bytes metadata)
  // of LtHash output value. We set a value of 50 bytes to allow some margin
  // in case of future changes to the outer digest.
  val MaxDigestSizeBytes: Int = 50

  val digestValidationParam: ValidationParam[PositiveInt] =
    ValidationParam(
      ProtocolVersion.nextValidationBoundPV -> PositiveInt.tryCreate(MaxDigestSizeBytes)
    )

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(
      v32.AcsCommitment
    )(
      supportedProtoVersionMemoizedPVV(_)(fromProtoV32),
      _.toProtoV32,
    )
  )

  private def fromProtoV32(
      pvv: ProtocolVersionValidation,
      protoMsg: v32.AcsCommitment,
  )(
      bytes: ByteString
  ): ParsingResult[AcsCommitment] =
    for {
      synchronizerId <- ProtoValidation.validateThen(
        protoMsg.physicalSynchronizerId,
        "physical_synchronizer_id",
        pvv,
      )(PhysicalSynchronizerId.fromProtoPrimitive)

      sender <- ProtoValidation.validateThen(
        protoMsg.sendingParticipantUid,
        "sending_participant_uid",
        pvv,
      )(parseLfParticipantId)

      counterparticipant <- ProtoValidation.validateThen(
        protoMsg.counterparticipantUid,
        "counterparticipant_uid",
        pvv,
      )(parseLfParticipantId)

      period <- parseRequired(CommitmentPeriod.fromProtoV32, "period", protoMsg.period)

      digest <- ProtoValidation.validateLength(
        protoMsg.digest,
        "digest",
        pvv,
        digestValidationParam,
      )

      rpv <- protocolVersionRepresentativeFor(ProtoVersion(32))
    } yield AcsCommitment(
      synchronizerId,
      sender,
      counterparticipant,
      period,
      digest,
    )(
      rpv,
      bytes.some,
    )

  def create(
      synchronizerId: PhysicalSynchronizerId,
      sender: LedgerParticipantId,
      counterparticipant: LedgerParticipantId,
      period: CommitmentPeriod,
      digest: Digest.DigestType,
      protocolVersion: ProtocolVersion,
  ): Either[String, AcsCommitment] = {
    val pvv = ProtocolVersionValidation.PV(protocolVersion)
    ProtoValidation
      .validateLength(digest, "digest", pvv, digestValidationParam)
      .leftMap(_.message)
      .map(
        AcsCommitment(
          synchronizerId,
          sender,
          counterparticipant,
          period,
          _,
        )(
          protocolVersionRepresentativeFor(protocolVersion),
          None,
        )
      )
  }

  @VisibleForTesting
  private[canton] def createUnsafe(
      synchronizerId: PhysicalSynchronizerId,
      sender: LedgerParticipantId,
      counterparticipant: LedgerParticipantId,
      period: CommitmentPeriod,
      digest: ByteString,
      protocolVersion: ProtocolVersion,
  ): AcsCommitment =
    new AcsCommitment(
      synchronizerId,
      sender,
      counterparticipant,
      period,
      digest,
    )(protocolVersionRepresentativeFor(protocolVersion), None)
}
