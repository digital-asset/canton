// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v32
import com.digitalasset.canton.serialization.ProtoConverter.{
  ParsingResult,
  parseLfParticipantId,
  parseRequiredNonEmpty,
}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.{ProtoUnvalidatedString, ProtoValidation}
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
import com.digitalasset.canton.{LedgerParticipantId, ProtoDeserializationError}
import com.google.protobuf.ByteString

final case class AcsCommitmentSummary(
    psid: PhysicalSynchronizerId,
    commitmentTick: CantonTimestamp,
    addressedCounterparticipants: Seq[LedgerParticipantId],
    unsentDigests: Seq[DigestForCounterparticipant],
    batchIndex: NonNegativeInt,
    lastBatch: Boolean,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      AcsCommitmentSummary.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends HasProtocolVersionedWrapper[AcsCommitmentSummary]
    with ProtocolVersionedMemoizedEvidence
    with PrettyPrinting {

  @transient override protected lazy val companionObj: AcsCommitmentSummary.type =
    AcsCommitmentSummary

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[AcsCommitmentSummary] =
    prettyOfClass(
      param("psid", _.psid),
      param("commitmentTick", _.commitmentTick),
      param("addressedCounterparticipants", _.addressedCounterparticipants),
      param("unsentDigests", _.unsentDigests),
      param("batchIndex", _.batchIndex),
      param("lastBatch", _.lastBatch),
    )

  private[messages] def toProtoV32: v32.AcsCommitmentSummary = v32.AcsCommitmentSummary(
    physicalSynchronizerId = psid.toProtoPrimitive,
    commitmentTick = commitmentTick.toProtoPrimitive,
    addressedCounterparticipants = addressedCounterparticipants.map(_.toProtoUnvalidated),
    unsentDigests = unsentDigests.map(_.toProtoV32),
    batchIndex = batchIndex.value,
    lastBatch = lastBatch,
  )
}

object AcsCommitmentSummary extends VersioningCompanionMemoization[AcsCommitmentSummary] {

  override def name: String = "AcsCommitmentSummary"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.v36)(v32.AcsCommitmentSummary)(
      supportedProtoVersionMemoizedPVV(_)(AcsCommitmentSummary.fromProtoV32),
      _.toProtoV32,
    ),
  )

  def create(
      psid: PhysicalSynchronizerId,
      commitmentTick: CantonTimestamp,
      addressedCounterparticipants: Seq[LedgerParticipantId],
      unsentDigests: Seq[DigestForCounterparticipant],
      batchIndex: NonNegativeInt,
      lastBatch: Boolean,
      protocolVersion: ProtocolVersion,
  ): AcsCommitmentSummary =
    AcsCommitmentSummary(
      psid,
      commitmentTick,
      addressedCounterparticipants,
      unsentDigests,
      batchIndex,
      lastBatch,
    )(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  def fromProtoV32(
      pvv: ProtocolVersionValidation,
      protoMsg: v32.AcsCommitmentSummary,
  )(
      bytes: ByteString
  ): ParsingResult[AcsCommitmentSummary] = for {
    psid <- ProtoValidation.validateThen(
      protoMsg.physicalSynchronizerId,
      "physical_synchronizer_id",
      pvv,
    )(PhysicalSynchronizerId.fromProtoPrimitive)
    commitmentTick <- CantonTimestamp.fromProtoPrimitive(protoMsg.commitmentTick)
    addressedCounterparticipants <- parseRequiredNonEmpty(
      (p: ProtoUnvalidatedString) =>
        ProtoValidation.validateThen(p, "addressed_counterparticipants", pvv)(parseLfParticipantId),
      "addressed_counterparticipants",
      protoMsg.addressedCounterparticipants,
    )
    unsentDigests <- protoMsg.unsentDigests.traverse(
      DigestForCounterparticipant.fromProtoV32(pvv, _)
    )
    batchIndex <- NonNegativeInt.create(protoMsg.batchIndex).leftMap { error =>
      ProtoDeserializationError.InvariantViolation("batch_index", error)
    }
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(32))
  } yield AcsCommitmentSummary(
    psid = psid,
    commitmentTick = commitmentTick,
    addressedCounterparticipants = addressedCounterparticipants,
    unsentDigests = unsentDigests,
    batchIndex = batchIndex,
    lastBatch = protoMsg.lastBatch,
  )(rpv, bytes.some)
}
