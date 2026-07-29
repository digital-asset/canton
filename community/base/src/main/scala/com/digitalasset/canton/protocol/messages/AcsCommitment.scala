// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.option.*
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.serialization.ProtoConverter.{
  ParsingResult,
  parseLfParticipantId,
  parseRequired,
}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  UnsupportedProtoCodec,
  VersionedProtoCodec,
  VersioningCompanionMemoization,
}
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
    with PrettyPrinting
    with NoCopy {

  @transient override protected lazy val companionObj: AcsCommitment.type = AcsCommitment

  private def toProtoV32: v32.AcsCommitment = v32.AcsCommitment(
    physicalSynchronizerId = psid.toProtoPrimitive,
    sendingParticipantUid = sender,
    counterparticipantUid = counterparticipant,
    period = period.toProtoV32.some,
    digest = digest,
  )

  override lazy val pretty: Pretty[AcsCommitment] =
    prettyOfClass(
      param("psid", _.psid),
      param("sender", _.sender),
      param("counterparticipant", _.counterparticipant),
      param("period", _.period),
      param("commitment", _.digest),
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString
}

object AcsCommitment extends VersioningCompanionMemoization[AcsCommitment] {
  override val name: String = "AcsCommitment"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(v32.AcsCommitment)(
      supportedProtoVersionMemoizedPVV(_)(fromProtoV32),
      _.toProtoV32,
    ),
  )

  private def fromProtoV32(
      pvv: ProtocolVersionValidation,
      protoMsg: v32.AcsCommitment,
  )(
      bytes: ByteString
  ): ParsingResult[AcsCommitment] = for {
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
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(32))
  } yield AcsCommitment(
    synchronizerId,
    sender,
    counterparticipant,
    period,
    protoMsg.digest,
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
  ): AcsCommitment =
    AcsCommitment(
      synchronizerId,
      sender,
      counterparticipant,
      period,
      digest,
    )(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )
}
