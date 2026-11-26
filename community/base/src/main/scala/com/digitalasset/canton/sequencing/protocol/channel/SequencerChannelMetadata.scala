// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol.channel

import cats.syntax.either.*
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfLedgerString, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting

final case class SequencerChannelMetadata(
    channelId: SequencerChannelId,
    initiatingMember: Member,
    receivingMember: Member,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SequencerChannelMetadata.type
    ]
) extends HasProtocolVersionedWrapper[SequencerChannelMetadata] {
  @transient override protected lazy val companionObj: SequencerChannelMetadata.type =
    SequencerChannelMetadata

  def toProtoV30: v30.SequencerChannelMetadata =
    v30.SequencerChannelMetadata(
      channelId.toProtoPrimitive,
      initiatingMember.toProtoPrimitive,
      receivingMember.toProtoPrimitive,
    )
}

object SequencerChannelMetadata extends VersioningCompanion[SequencerChannelMetadata] {
  override val name: String = "SequencerChannelMetadata"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.sequencerChannel)(
      v30.SequencerChannelMetadata
    )(
      supportedProtoVersionPVV(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def apply(
      channelId: SequencerChannelId,
      initiatingMember: Member,
      receivingMember: Member,
      protocolVersion: ProtocolVersion,
  ): SequencerChannelMetadata =
    SequencerChannelMetadata(channelId, initiatingMember, receivingMember)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      pvv: ProtocolVersionValidation,
      sequencerChannelMetadataP: v30.SequencerChannelMetadata,
  ): ParsingResult[SequencerChannelMetadata] = {
    val v30.SequencerChannelMetadata(channelIdP, initiatingMemberP, receivingMemberP) =
      sequencerChannelMetadataP
    for {
      channelId <- ProtoValidation.validateThen(channelIdP, "channel_id", pvv)(
        SequencerChannelId.fromProtoPrimitive
      )
      initiatingMember <- ProtoValidation.validateThen(
        initiatingMemberP,
        "initiating_member",
        pvv,
      )(Member.fromProtoPrimitive)
      receivingMember <- ProtoValidation.validateThen(
        receivingMemberP,
        "receiving_member",
        pvv,
      )(Member.fromProtoPrimitive)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SequencerChannelMetadata(channelId, initiatingMember, receivingMember)(rpv)
  }
}

final case class SequencerChannelId private (channelId: String68)
    extends AnyVal
    with PrettyPrintingFromCompanion {
  def unwrap: String = channelId.unwrap
  def toProtoPrimitive: String = channelId.toProtoPrimitive

  override def prettyCompanion: PrettyPrintingCompanion[SequencerChannelId] = SequencerChannelId
}

object SequencerChannelId extends PrettyPrintingCompanion[SequencerChannelId] {
  override protected val pretty: Pretty[SequencerChannelId] =
    prettyOfString(chid => chid.channelId.str.show)

  def create(s: String, field: Option[String]): ParsingResult[SequencerChannelId] =
    String68.fromProtoPrimitive(s, field).flatMap(create(_))
  def create(s: String68, field: Option[String] = None): ParsingResult[SequencerChannelId] =
    LfLedgerString
      .fromString(s.str)
      .leftMap(ProtoDeserializationError.StringConversionError(_, field))
      .map(_ => SequencerChannelId(s))

  @VisibleForTesting
  def tryCreate(s: String): SequencerChannelId =
    create(s, None).valueOr(err => throw new IllegalArgumentException(err.message))

  @VisibleForTesting
  def tryCreate(s: String68): SequencerChannelId =
    create(s, None).valueOr(err => throw new IllegalArgumentException(err.message))

  def fromProtoPrimitive(channelId: String): ParsingResult[SequencerChannelId] =
    create(channelId, field = None)

  def fromProtoPrimitive(channelId: String, field: String): ParsingResult[SequencerChannelId] =
    create(channelId, Some(field))

}
