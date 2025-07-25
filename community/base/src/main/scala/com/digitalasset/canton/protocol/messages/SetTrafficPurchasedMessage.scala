// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.protocol.v30.TypedSignedProtocolMessageContent
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

final case class SetTrafficPurchasedMessage private (
    member: Member,
    serial: PositiveInt,
    totalTrafficPurchased: NonNegativeLong,
    synchronizerId: PhysicalSynchronizerId,
)(
    override val deserializedFrom: Option[ByteString]
) extends ProtocolVersionedMemoizedEvidence
    with HasProtocolVersionedWrapper[SetTrafficPurchasedMessage]
    with PrettyPrinting
    with SignedProtocolMessageContent {

  val representativeProtocolVersion: RepresentativeProtocolVersion[
    SetTrafficPurchasedMessage.type
  ] = SetTrafficPurchasedMessage.protocolVersionRepresentativeFor(synchronizerId.protocolVersion)

  // Only used in security tests, this is not part of the protobuf payload
  override val signingTimestamp: Option[CantonTimestamp] = None

  @transient override protected lazy val companionObj: SetTrafficPurchasedMessage.type =
    SetTrafficPurchasedMessage

  def toProtoV30: v30.SetTrafficPurchasedMessage =
    v30.SetTrafficPurchasedMessage(
      member = member.toProtoPrimitive,
      serial = serial.value,
      totalTrafficPurchased = totalTrafficPurchased.value,
      physicalSynchronizerId = synchronizerId.toProtoPrimitive,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override protected[messages] def toProtoTypedSomeSignedProtocolMessage
      : TypedSignedProtocolMessageContent.SomeSignedProtocolMessage =
    v30.TypedSignedProtocolMessageContent.SomeSignedProtocolMessage.SetTrafficPurchased(
      getCryptographicEvidence
    )

  override protected def pretty: Pretty[SetTrafficPurchasedMessage] = prettyOfClass(
    param("member", _.member),
    param("serial", _.serial),
    param("totalTrafficPurchased", _.totalTrafficPurchased),
    param("synchronizerId", _.synchronizerId),
  )
}

object SetTrafficPurchasedMessage
    extends VersioningCompanionMemoization[
      SetTrafficPurchasedMessage,
    ] {
  override val name: String = "SetTrafficPurchasedMessage"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(1) -> VersionedProtoCodec(ProtocolVersion.v34)(
      v30.SetTrafficPurchasedMessage
    )(
      supportedProtoVersionMemoized(_)(fromProtoV30),
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
      proto: v30.SetTrafficPurchasedMessage
  )(bytes: ByteString): ParsingResult[SetTrafficPurchasedMessage] =
    for {
      member <- Member.fromProtoPrimitive(proto.member, "member")
      serial <- ProtoConverter.parsePositiveInt("serial", proto.serial)
      totalTrafficPurchased <- ProtoConverter.parseNonNegativeLong(
        "total_traffic_purchased",
        proto.totalTrafficPurchased,
      )
      synchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(
        proto.physicalSynchronizerId,
        "physical_synchronizer_id",
      )
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
