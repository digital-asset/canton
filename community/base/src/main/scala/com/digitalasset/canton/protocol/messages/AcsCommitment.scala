// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import cats.syntax.option.*
import com.digitalasset.canton.ProtoDeserializationError.CryptoDeserializationError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseRequired}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  UnsupportedProtoCodec,
  VersionedProtoCodec,
  VersioningCompanionMemoization,
}
import com.digitalasset.canton.{LedgerParticipantId, ProtoDeserializationError}
import com.google.protobuf.ByteString

final case class AcsCommitment private (
    psid: PhysicalSynchronizerId,
    sender: LedgerParticipantId,
    counterparticipant: LedgerParticipantId,
    period: CommitmentPeriod,
    digest: Digest.HashedDigestType,
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
    digest = Digest.hashedDigestTypeToProto(digest),
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
      supportedProtoVersionMemoized(_)(fromProtoV32),
      _.toProtoV32,
    ),
  )

  private def fromProtoV32(protoMsg: v32.AcsCommitment)(
      bytes: ByteString
  ): ParsingResult[AcsCommitment] = for {
    synchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(
      protoMsg.physicalSynchronizerId,
      "physical_synchronizer_id",
    )
    sender <- LedgerParticipantId
      .fromString(protoMsg.sendingParticipantUid)
      .leftMap(ProtoDeserializationError.StringConversionError(_))
    counterparticipant <- LedgerParticipantId
      .fromString(protoMsg.counterparticipantUid)
      .leftMap(ProtoDeserializationError.StringConversionError(_))

    period <- parseRequired(CommitmentPeriod.fromProtoV32, "period", protoMsg.period)

    cmt = protoMsg.digest
    commitment <- Digest
      .hashedDigestTypeFromByteString(cmt)
      .leftMap(
        CryptoDeserializationError.apply
      )
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(32))
  } yield AcsCommitment(synchronizerId, sender, counterparticipant, period, commitment)(
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
      Digest.hashDigest(digest),
    )(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )
}
