// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party.acsreplication

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{PartyId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.version.*
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString

import AcsReplicationSourceParticipantMessage.DataOrStatus

final case class AcsReplicationSourceParticipantMessage(dataOrStatus: DataOrStatus)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      AcsReplicationSourceParticipantMessage.type
    ]
) extends HasProtocolVersionedWrapper[AcsReplicationSourceParticipantMessage] {
  @transient override protected lazy val companionObj: AcsReplicationSourceParticipantMessage.type =
    AcsReplicationSourceParticipantMessage

  def toProtoV30: v30.AcsReplicationSourceParticipantMessage =
    v30.AcsReplicationSourceParticipantMessage(dataOrStatus.toProtoV30)
}

object AcsReplicationSourceParticipantMessage
    extends VersioningCompanionContext[AcsReplicationSourceParticipantMessage, ProtocolVersion] {
  override val name: String = "PartyReplicationSourceParticipantMessage"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.dev)(
      v30.AcsReplicationSourceParticipantMessage
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      proto: v30.AcsReplicationSourceParticipantMessage,
  ): ParsingResult[AcsReplicationSourceParticipantMessage] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    dataOrStatus <- proto.dataOrStatus match {
      case v30.AcsReplicationSourceParticipantMessage.DataOrStatus.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("data_or_status"))
      case v30.AcsReplicationSourceParticipantMessage.DataOrStatus.AcsBatch(batchP) =>
        for {
          contracts <- batchP.contracts.toList.traverse(ActiveContract.fromProtoV30)
          nonEmptyContracts <- NonEmpty
            .from(contracts)
            .toRight(
              ProtoDeserializationError
                .ValueConversionError("contracts", "Contracts must not be empty")
            )
        } yield AcsBatch(nonEmptyContracts)
      case v30.AcsReplicationSourceParticipantMessage.DataOrStatus
            .EndOfAcs(
              v30.AcsReplicationSourceParticipantMessage.EndOfAcs(acsDigestByteString, signatureP)
            ) =>
        for {
          acsDigest <- AcsDigest.fromByteString(expectedProtocolVersion, acsDigestByteString)
          signature <- ProtoConverter.parseRequired(
            Signature.fromProtoV30,
            "signature",
            signatureP,
          )
        } yield EndOfAcs(acsDigest, acsDigestByteString, signature): DataOrStatus
    }
  } yield AcsReplicationSourceParticipantMessage(dataOrStatus)(rpv)

  sealed trait DataOrStatus {
    def toProtoV30: v30.AcsReplicationSourceParticipantMessage.DataOrStatus
  }

  final case class AcsBatch(contracts: NonEmpty[Seq[ActiveContract]]) extends DataOrStatus {
    override def toProtoV30: v30.AcsReplicationSourceParticipantMessage.DataOrStatus =
      v30.AcsReplicationSourceParticipantMessage.DataOrStatus.AcsBatch(
        v30.AcsReplicationSourceParticipantMessage
          .AcsBatch(contracts.forgetNE.map(_.toProtoV30))
      )
  }

  final case class GetAcsArguments(
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      asOf: CantonTimestamp,
      excludedStakeholders: Set[PartyId],
  ) {
    def toProtoV30: v30.GetAcsArguments =
      v30.GetAcsArguments(
        partyId.toProtoPrimitive,
        synchronizerId.toProtoPrimitive,
        Some(asOf.toProtoTimestamp),
        excludedStakeholders.map(_.toProtoPrimitive).toSeq,
      )

  }

  object GetAcsArguments {
    def fromProtoV30(
        proto: v30.GetAcsArguments
    ): ParsingResult[GetAcsArguments] = for {
      partyId <- PartyId.fromProtoPrimitive(proto.partyId, "party_id")
      synchronizerId <- SynchronizerId.fromProtoPrimitive(proto.synchronizerId, "synchronizer_id")
      asOf <- ProtoConverter
        .parseRequired(CantonTimestamp.fromProtoTimestamp, "as_of", proto.asOf)
      excludedStakeholders <- proto.excludedStakeholders.traverse(
        PartyId.fromProtoPrimitive(_, "excluded_stakeholders")
      )
    } yield GetAcsArguments(partyId, synchronizerId, asOf, excludedStakeholders.toSet)
  }

  final case class AcsDigest(
      acsHash: ByteString,
      getAcsArgs: GetAcsArguments,
      sourceParticipantUid: UniqueIdentifier,
      agreedAt: CantonTimestamp,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        AcsDigest.type
      ]
  ) extends HasProtocolVersionedWrapper[AcsDigest] {

    @transient override protected lazy val companionObj: AcsDigest.type = AcsDigest

    def toProtoV30: v30.AcsDigest =
      v30.AcsDigest(
        acsHash,
        Some(getAcsArgs.toProtoV30),
        sourceParticipantUid.toProtoPrimitive,
        Some(agreedAt.toProtoTimestamp),
      )
  }

  object AcsDigest extends VersioningCompanion[AcsDigest] {
    override def name: String = "AcsDigest"

    override val versioningTable: VersioningTable = VersioningTable(
      ProtoVersion(-1) -> UnsupportedProtoCodec(),
      ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.dev)(
        v30.AcsDigest
      )(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30,
      ),
    )

    def fromProtoV30(
        proto: v30.AcsDigest
    ): ParsingResult[AcsDigest] = for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      acsHash = proto.acsHash
      getAcsArgs <- ProtoConverter.parseRequired(
        GetAcsArguments.fromProtoV30,
        "get_acs_args",
        proto.getAcsArgs,
      )
      sourceParticipantUid <- UniqueIdentifier.fromProtoPrimitive(
        proto.sourceParticipantUid,
        "source_participant_uid",
      )
      agreedAt <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "request_made_at",
        proto.agreedAt,
      )
    } yield AcsDigest(acsHash, getAcsArgs, sourceParticipantUid, agreedAt)(rpv)

    def apply(
        acsHash: ByteString,
        getAcsArgs: GetAcsArguments,
        sourceParticipantUid: UniqueIdentifier,
        agreedAt: CantonTimestamp,
        protocolVersion: ProtocolVersion,
    ): AcsDigest =
      AcsDigest(acsHash, getAcsArgs, sourceParticipantUid, agreedAt)(
        protocolVersionRepresentativeFor(protocolVersion)
      )
  }

  final case class EndOfAcs(
      acsDigest: AcsDigest,
      acsDigestByteString: ByteString,
      signature: Signature,
  ) extends DataOrStatus {
    override def toProtoV30: v30.AcsReplicationSourceParticipantMessage.DataOrStatus =
      v30.AcsReplicationSourceParticipantMessage.DataOrStatus.EndOfAcs(
        v30.AcsReplicationSourceParticipantMessage.EndOfAcs(
          acsDigestByteString,
          Some(signature.toProtoV30),
        )
      )
  }

  def apply(
      dataOrStatus: DataOrStatus,
      protocolVersion: ProtocolVersion,
  ): AcsReplicationSourceParticipantMessage =
    AcsReplicationSourceParticipantMessage(dataOrStatus)(
      protocolVersionRepresentativeFor(protocolVersion)
    )
}
