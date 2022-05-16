// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protobuf.{VersionedMessageV0, VersionedMessageV1, VersionedMessageV2}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec
import cats.syntax.either._
import com.digitalasset.canton.version.HasProtocolVersionedWrapperTest.{Message, protocolVersion}

class HasProtocolVersionedWrapperTest extends AnyWordSpec with BaseTest {

  /*
      proto               0         1    2
      protocolVersion     2    3    4    5    6  ...
   */
  "HasVersionedWrapperV2" should {
    "use correct proto version depending on the protocol version for serialization" in {
      def message(i: Int): Message = Message("Hey", 1, 2.0, protocolVersion(i))(None)
      message(2).toProtoVersioned.version shouldBe 0
      message(3).toProtoVersioned.version shouldBe 0
      message(4).toProtoVersioned.version shouldBe 1
      message(5).toProtoVersioned.version shouldBe 2
      message(6).toProtoVersioned.version shouldBe 2
    }

    "set correct protocol version depending on the proto version" in {
      def fromByteString(bytes: ByteString, protoVersion: Int): Message = Message
        .fromByteString(
          VersionedMessage[Message](bytes, protoVersion).toByteString
        )
        .value

      val messageV0 = VersionedMessageV0("Hey").toByteString
      val expectedV0Deserialization = Message("Hey", 0, 0, protocolVersion(2))(None)
      fromByteString(messageV0, 0) shouldBe expectedV0Deserialization

      val messageV1 = VersionedMessageV1("Hey", 42).toByteString
      val expectedV1Deserialization = Message("Hey", 42, 1.0, protocolVersion(4))(None)
      fromByteString(messageV1, 1) shouldBe expectedV1Deserialization

      val messageV2 = VersionedMessageV2("Hey", 42, 43.0).toByteString
      val expectedV2Deserialization = Message("Hey", 42, 43.0, protocolVersion(5))(None)
      fromByteString(messageV2, 2) shouldBe expectedV2Deserialization
    }

    "return the protocol representative" in {
      def representative(i: Int): ProtocolVersion =
        Message.supportedProtoVersions.protocolVersionRepresentativeFor(protocolVersion(i))

      representative(2) shouldBe protocolVersion(2)
      representative(3) shouldBe protocolVersion(2)
      representative(4) shouldBe protocolVersion(4)
      representative(5) shouldBe protocolVersion(5)
      representative(6) shouldBe protocolVersion(5)
      representative(7) shouldBe protocolVersion(5)
    }
  }
}

object HasProtocolVersionedWrapperTest {
  private def protocolVersion(i: Int): ProtocolVersion = ProtocolVersion(i, 0, 0)

  case class Message(
      msg: String,
      iValue: Int,
      dValue: Double,
      representativeProtocolVersion: ProtocolVersion,
  )(
      val deserializedFrom: Option[ByteString] = None
  ) extends HasProtocolVersionedWrapper[VersionedMessage[Message]] {
    override def toProtoVersioned: VersionedMessage[Message] = Message.toProtoVersioned(this)
    override def getCryptographicEvidence: ByteString =
      super[HasProtocolVersionedWrapper].toByteString

    def toProtoV0 = VersionedMessageV0(msg)
    def toProtoV1 = VersionedMessageV1(msg, iValue)
    def toProtoV2 = VersionedMessageV2(msg, iValue, dValue)
  }

  object Message extends HasMemoizedProtocolVersionedWrapperCompanion[Message] {
    def name: String = "Message"

    /*
      proto               0         1    2
      protocolVersion     2    3    4    5    6  ...
     */
    val supportedProtoVersions = SupportedProtoVersions(
      1 -> VersionedProtoConverter(
        protocolVersion(4),
        supportedProtoVersionMemoized(VersionedMessageV1)(fromProtoV1),
        _.toProtoV1.toByteString,
      ),
      0 -> VersionedProtoConverter(
        protocolVersion(2),
        supportedProtoVersionMemoized(VersionedMessageV0)(fromProtoV0),
        _.toProtoV0.toByteString,
      ),
      2 -> VersionedProtoConverter(
        protocolVersion(5),
        supportedProtoVersionMemoized(VersionedMessageV2)(fromProtoV2),
        _.toProtoV2.toByteString,
      ),
    )

    def fromProtoV0(message: VersionedMessageV0)(bytes: ByteString): ParsingResult[Message] =
      Message(message.msg, 0, 0, supportedProtoVersions.protocolVersionRepresentativeFor(0))(
        Some(bytes)
      ).asRight

    def fromProtoV1(message: VersionedMessageV1)(bytes: ByteString): ParsingResult[Message] =
      Message(
        message.msg,
        message.value,
        1,
        supportedProtoVersions.protocolVersionRepresentativeFor(1),
      )(
        Some(bytes)
      ).asRight

    def fromProtoV2(message: VersionedMessageV2)(bytes: ByteString): ParsingResult[Message] =
      Message(
        message.msg,
        message.iValue,
        message.dValue,
        supportedProtoVersions.protocolVersionRepresentativeFor(2),
      )(
        Some(bytes)
      ).asRight
  }
}
