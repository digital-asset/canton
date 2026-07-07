// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.protobuf.{VersionedMessageV0, VersionedMessageV1, VersionedMessageV2}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn

/*
  Supposing that basePV is 30, we get the scheme

   proto               0           1     2
   protocolVersion     30    31    32    33    34  ...
 */
class HasVersionedWrapperTest extends AnyWordSpec with BaseTest {

  import HasVersionedWrapperTest.*

  "HasVersionedWrapper" should {
    "use correct proto version depending on the protocol version for serialization" in {
      val message = Message("Hey", 1, 2.0)

      message.toProtoVersioned(basePV).version shouldBe 0
      message.toProtoVersioned(basePV + 1).version shouldBe 0
      message.toProtoVersioned(basePV + 2).version shouldBe 1
      message.toProtoVersioned(basePV + 3).version shouldBe 2
      message.toProtoVersioned(basePV + 4).version shouldBe 2
    }

    "set correct protocol version depending on the proto version" in {
      def fromByteString(
          bytes: ByteString,
          protoVersion: Int,
      ): ParsingResult[Message] = Message.fromTrustedByteString(
        VersionedMessage[Message](bytes, protoVersion).toByteString
      )

      val messageV1 = VersionedMessageV1("Hey", 42).toByteString
      val expectedV1Deserialization = Message("Hey", 42, 1.0)
      fromByteString(messageV1, 1).value shouldBe expectedV1Deserialization

      // Round trip serialization
      Message
        .fromTrustedByteString(expectedV1Deserialization.toByteString(basePV + 2))
        .value shouldBe expectedV1Deserialization

      val messageV2 = VersionedMessageV2("Hey", 42, 43.0).toByteString
      val expectedV2Deserialization = Message("Hey", 42, 43.0)
      fromByteString(messageV2, 2).value shouldBe expectedV2Deserialization

      // Round trip serialization
      Message
        .fromTrustedByteString(expectedV2Deserialization.toByteString(basePV + 3))
        .value shouldBe expectedV2Deserialization
    }
  }

  "HasVersionedWrapperE" should {
    "return an error when serialization is not possible" in {
      val message = MessageE("Hey", 1, 2.0)

      message.toProtoVersioned(basePV).value.version shouldBe 0
      message.toProtoVersioned(basePV + 1).value.version shouldBe 0
      message.toProtoVersioned(basePV + 2).value.version shouldBe 1
      message.toProtoVersioned(basePV + 3).left.value shouldBe "Nope"
      message.toProtoVersioned(basePV + 4).left.value shouldBe "Nope"
    }
  }
}

object HasVersionedWrapperTest {

  private val basePV = ProtocolVersion.minimum

  implicit class RichProtocolVersion(val pv: ProtocolVersion) {
    def +(i: Int): ProtocolVersion = ProtocolVersion(pv.v + i)
  }

  final case class Message(
      msg: String,
      iValue: Int,
      dValue: Double,
  ) extends HasVersionedWrapper[Message] {

    @transient override protected lazy val companionObj: Message.type = Message

    def toProtoV0 = VersionedMessageV0(msg)
    def toProtoV1 = VersionedMessageV1(msg, iValue)
    def toProtoV2 = VersionedMessageV2(msg, iValue, dValue)
  }

  object Message extends HasVersionedMessageCompanion[Message] {
    def name: String = "Message"

    override def supportedProtoVersions: SupportedProtoVersions =
      SupportedProtoVersions(
        ProtoVersion(1) -> ProtoCodec(
          ProtocolVersion.createAlpha((basePV + 2).v),
          supportedProtoVersion(VersionedMessageV1)(fromProtoV1),
          _.toProtoV1,
        ),
        ProtoVersion(0) -> ProtoCodec(
          ProtocolVersion.createAlpha(basePV.v),
          supportedProtoVersion(VersionedMessageV0)(fromProtoV0),
          _.toProtoV0,
        ),
        ProtoVersion(2) -> ProtoCodec(
          ProtocolVersion.createAlpha((basePV + 3).v),
          supportedProtoVersion(VersionedMessageV2)(fromProtoV2),
          _.toProtoV2,
        ),
      )

    def fromProtoV0(message: VersionedMessageV0): ParsingResult[Message] =
      Message(message.msg, 0, 0).asRight

    def fromProtoV1(message: VersionedMessageV1): ParsingResult[Message] =
      Message(message.msg, message.value, 1).asRight

    def fromProtoV2(message: VersionedMessageV2): ParsingResult[Message] =
      Message(message.msg, message.iValue, message.dValue).asRight
  }

  final case class MessageE(
      msg: String,
      iValue: Int,
      dValue: Double,
  ) extends HasVersionedWrapperE[MessageE] {

    @transient override protected lazy val companionObj: MessageE.type = MessageE

    def toProtoV0: Either[String, VersionedMessageV0] = VersionedMessageV0(msg).asRight
    def toProtoV1: Either[String, VersionedMessageV1] = VersionedMessageV1(msg, iValue).asRight
    def toProtoV2: Either[String, scalapb.GeneratedMessage] = "Nope".asLeft
  }

  object MessageE extends HasVersionedMessageCompanionE[MessageE] {
    def name: String = "Message"

    override def supportedProtoVersions: SupportedProtoVersions =
      SupportedProtoVersions(
        ProtoVersion(1) -> ProtoCodec(
          ProtocolVersion.createAlpha((basePV + 2).v),
          supportedProtoVersion(VersionedMessageV1)(fromProtoV1),
          _.toProtoV1,
        ),
        ProtoVersion(0) -> ProtoCodec(
          ProtocolVersion.createAlpha(basePV.v),
          supportedProtoVersion(VersionedMessageV0)(fromProtoV0),
          _.toProtoV0,
        ),
        ProtoVersion(2) -> ProtoCodec(
          ProtocolVersion.createAlpha((basePV + 3).v),
          supportedProtoVersion(VersionedMessageV2)(fromProtoV2),
          _.toProtoV2,
        ),
      )

    def fromProtoV0(message: VersionedMessageV0): ParsingResult[MessageE] =
      MessageE(message.msg, 0, 0).asRight

    def fromProtoV1(message: VersionedMessageV1): ParsingResult[MessageE] =
      MessageE(message.msg, message.value, 1).asRight

    @nowarn("msg=parameter .* in method .* is never used")
    def fromProtoV2(message: VersionedMessageV2): ParsingResult[MessageE] =
      OtherError("No deserialization from v2").asLeft
  }
}
