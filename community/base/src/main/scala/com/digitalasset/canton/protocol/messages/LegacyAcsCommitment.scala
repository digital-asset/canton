// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.CryptoDeserializationError
import com.digitalasset.canton.data.{
  AcsCommitmentData,
  BufferedAcsCommitment,
  CantonTimestamp,
  CantonTimestampSecond,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionMemoization,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.*

/** A commitment to the active contract set (ACS) that is shared between two participants on a given
  * synchronizer at a given time.
  *
  * Given a commitment scheme to the ACS, the semantics are as follows: the sender declares that the
  * shared ACS was exactly the one committed to, at every commitment tick during the specified
  * period and as determined by the period's interval.
  *
  * The interval is assumed to be a round number of seconds. The ticks then start at the Java EPOCH
  * time, and are exactly `interval` apart.
  */
abstract sealed case class LegacyAcsCommitment private (
    psid: PhysicalSynchronizerId,
    sender: ParticipantId,
    counterParticipant: ParticipantId,
    period: LegacyCommitmentPeriod,
    commitment: Digest.HashedDigestType,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      LegacyAcsCommitment.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends HasProtocolVersionedWrapper[LegacyAcsCommitment]
    with AcsCommitmentData
    with SignedProtocolMessageContent
    with NoCopy {

  @transient override protected lazy val companionObj: LegacyAcsCommitment.type =
    LegacyAcsCommitment

  override def signingTimestamp: Option[CantonTimestamp] = Some(period.toInclusive.forgetRefinement)

  protected def toProtoV30: v30.AcsCommitment =
    v30.AcsCommitment(
      physicalSynchronizerId = psid.toProtoPrimitive,
      sendingParticipantUid = sender.uid.toProtoPrimitive,
      counterParticipantUid = counterParticipant.uid.toProtoPrimitive,
      fromExclusive = period.fromExclusive.toProtoPrimitive,
      toInclusive = period.toInclusive.toProtoPrimitive,
      commitment = Digest.hashedDigestTypeToProto(commitment),
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override protected[messages] def toProtoTypedSomeSignedProtocolMessageV30
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.AcsCommitment(
      getCryptographicEvidence
    )

  override lazy val pretty: Pretty[LegacyAcsCommitment] =
    prettyOfClassWithName("AscCommitment")(
      param("psid", _.psid),
      param("sender", _.sender),
      param("counterParticipant", _.counterParticipant),
      param("period", _.period),
      param("commitment", _.commitment),
    )

  def toQueuedAcsCommitment: BufferedAcsCommitment = this
    .into[BufferedAcsCommitment]
    .withFieldComputed(_.synchronizerId, _.psid.logical)
    .transform
}

object LegacyAcsCommitment extends VersioningCompanionMemoization[LegacyAcsCommitment] {
  override val name: String = "LegacyAcsCommitment"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.AcsCommitment)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  @VisibleForTesting
  def create(
      synchronizerId: PhysicalSynchronizerId,
      sender: ParticipantId,
      counterParticipant: ParticipantId,
      period: LegacyCommitmentPeriod,
      commitment: Digest.HashedDigestType,
      protocolVersion: ProtocolVersion,
  ): LegacyAcsCommitment =
    new LegacyAcsCommitment(
      synchronizerId,
      sender,
      counterParticipant,
      period,
      commitment,
    )(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    ) {}

  def create(
      synchronizerId: PhysicalSynchronizerId,
      sender: ParticipantId,
      counterParticipant: ParticipantId,
      period: LegacyCommitmentPeriod,
      commitment: Digest.DigestType,
      protocolVersion: ProtocolVersion,
  ): LegacyAcsCommitment =
    new LegacyAcsCommitment(
      synchronizerId,
      sender,
      counterParticipant,
      period,
      Digest.hashDigest(commitment),
    )(
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    ) {}

  private def fromProtoV30(protoMsg: v30.AcsCommitment)(
      bytes: ByteString
  ): ParsingResult[LegacyAcsCommitment] =
    for {
      synchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(
        protoMsg.physicalSynchronizerId,
        "AcsCommitment.physical_synchronizer_id",
      )
      sender <- UniqueIdentifier
        .fromProtoPrimitive(
          protoMsg.sendingParticipantUid,
          "AcsCommitment.sending_participant_uid",
        )
        .map(ParticipantId(_))
      counterParticipant <- UniqueIdentifier
        .fromProtoPrimitive(
          protoMsg.counterParticipantUid,
          "AcsCommitment.counter_participant_uid",
        )
        .map(ParticipantId(_))
      fromExclusive <- CantonTimestampSecond.fromProtoPrimitive(
        "from_exclusive",
        protoMsg.fromExclusive,
      )
      toInclusive <- CantonTimestampSecond.fromProtoPrimitive("to_inclusive", protoMsg.toInclusive)

      periodLength <- PositiveSeconds
        .between(fromExclusive, toInclusive)
        .leftMap { _ =>
          ProtoDeserializationError.InvariantViolation(
            field = None,
            error = s"Illegal commitment period length: $fromExclusive, $toInclusive",
          )
        }

      period = LegacyCommitmentPeriod(fromExclusive, periodLength)
      cmt = protoMsg.commitment
      commitment <- Digest
        .hashedDigestTypeFromByteString(cmt)
        .leftMap(
          CryptoDeserializationError.apply
        )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield new LegacyAcsCommitment(synchronizerId, sender, counterParticipant, period, commitment)(
      rpv,
      Some(bytes),
    ) {}

  implicit val acsCommitmentCast: SignedMessageContentCast[LegacyAcsCommitment] =
    SignedMessageContentCast.create[LegacyAcsCommitment]("LegacyAcsCommitment") {
      case m: LegacyAcsCommitment => Some(m)
      case _ => None
    }
}
