// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrintingCompanion}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

final case class SetTrafficPurchasedMessage private (
    member: Member,
    serial: PositiveInt,
    totalTrafficPurchased: NonNegativeLong,
    psid: PhysicalSynchronizerId,
)(
    override val deserializedFrom: Option[ByteString]
) extends ProtocolVersionedMemoizedEvidence
    with HasProtocolVersionedWrapper[SetTrafficPurchasedMessage]
    with SignedProtocolMessageContent {

  val representativeProtocolVersion: RepresentativeProtocolVersion[
    SetTrafficPurchasedMessage.type
  ] = SetTrafficPurchasedMessage.protocolVersionRepresentativeFor(psid.protocolVersion)

  // Only used in security tests, this is not part of the protobuf payload
  override val signingTimestamp: Option[CantonTimestamp] = None

  @transient override protected lazy val companionObj: SetTrafficPurchasedMessage.type =
    SetTrafficPurchasedMessage

  def toProtoV30: v30.SetTrafficPurchasedMessage =
    v30.SetTrafficPurchasedMessage(
      member = member.toProtoPrimitive,
      serial = serial.value,
      totalTrafficPurchased = totalTrafficPurchased.value,
      physicalSynchronizerId = psid.toProtoPrimitive,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override protected[messages] def toProtoTypedSomeSignedProtocolMessageV30
      : v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.SetTrafficPurchased(
      getCryptographicEvidence
    )

  override def prettyCompanion: PrettyPrintingCompanion[SetTrafficPurchasedMessage] =
    SetTrafficPurchasedMessage
}

object SetTrafficPurchasedMessage
    extends VersioningCompanionMemoization[
      SetTrafficPurchasedMessage,
    ]
    with PrettyPrintingCompanion[SetTrafficPurchasedMessage] {
  override val name: String = "SetTrafficPurchasedMessage"

  override protected val pretty: Pretty[SetTrafficPurchasedMessage] = prettyOfClass(
    param("member", _.member),
    param("serial", _.serial),
    param("totalTrafficPurchased", _.totalTrafficPurchased),
    param("psid", _.psid),
  )

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(1) -> VersionedProtoCodec(ProtocolVersion.v35)(
      v30.SetTrafficPurchasedMessage
    )(
      supportedProtoVersionMemoizedPVV(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def apply(
      member: Member,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      psid: PhysicalSynchronizerId,
  ): SetTrafficPurchasedMessage =
    new SetTrafficPurchasedMessage(member, serial, totalTrafficPurchased, psid)(
      None
    )

  def fromProtoV30(
      pvv: ProtocolVersionValidation,
      proto: v30.SetTrafficPurchasedMessage,
  )(bytes: ByteString): ParsingResult[SetTrafficPurchasedMessage] =
    for {
      member <- ProtoValidation.validateThen(proto.member, "member", pvv)(
        Member.fromProtoPrimitive
      )
      serial <- ProtoConverter.parsePositiveInt("serial", proto.serial)
      totalTrafficPurchased <- ProtoConverter.parseNonNegativeLong(
        "total_traffic_purchased",
        proto.totalTrafficPurchased,
      )
      synchronizerId <- ProtoValidation.validateThen(
        proto.physicalSynchronizerId,
        "physical_synchronizer_id",
        pvv,
      )(PhysicalSynchronizerId.fromProtoPrimitive)
    } yield new SetTrafficPurchasedMessage(
      member,
      serial,
      totalTrafficPurchased,
      synchronizerId,
    )(Some(bytes))

  implicit val setTrafficPurchasedCast: SignedMessageContentCast[SetTrafficPurchasedMessage] =
    SignedMessageContentCast.create[SetTrafficPurchasedMessage](
      "SetTrafficPurchasedMessage"
    ) {
      case m: SetTrafficPurchasedMessage => Some(m)
      case _ => None
    }
}
