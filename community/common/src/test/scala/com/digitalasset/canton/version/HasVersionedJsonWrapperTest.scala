// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protobuf.{VersionedMessageV0, VersionedMessageV1, VersionedMessageV2}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.scalatest.wordspec.AnyWordSpec
import scalapb.json4s.JsonFormat

/*
  Supposing that basePV is 30, we get the scheme

   proto               0           1     2
   protocolVersion     30    31    32    33    34  ...
 */
class HasVersionedJsonWrapperTest extends AnyWordSpec with BaseTest {

  import HasVersionedJsonWrapperTest.*

  "HasVersionedWrapper" should {
    "use correct proto version depending on the protocol version for serialization" in {
      val message = JsonMessage("Hey", 1, 2.0)

      message.toProtoVersioned(basePV).version shouldBe 0
      message.toProtoVersioned(basePV + 1).version shouldBe 0
      message.toProtoVersioned(basePV + 2).version shouldBe 1
      message.toProtoVersioned(basePV + 3).version shouldBe 2
      message.toProtoVersioned(basePV + 4).version shouldBe 2
    }

    "set correct protocol version depending on the proto version" in {
      def fromJsonString(
          jvalue: JValue,
          protoVersion: Int,
      ): ParsingResult[JsonMessage] = JsonMessage.fromTrustedJsonString(
        JsonMethods.compact(VersionedJsonMessage[JsonMessage](jvalue, protoVersion).toJson)
      )

      val messageV1 = JsonFormat.toJson(VersionedMessageV1("Hey", 42))
      val expectedV1Deserialization = JsonMessage("Hey", 42, 1.0)
      fromJsonString(messageV1, 1).value shouldBe expectedV1Deserialization

      // Round trip serialization
      JsonMessage
        .fromTrustedJsonString(expectedV1Deserialization.toJsonString(basePV + 2))
        .value shouldBe expectedV1Deserialization

      val messageV2 = JsonFormat.toJson(VersionedMessageV2("Hey", 42, 43.0))
      val expectedV2Deserialization = JsonMessage("Hey", 42, 43.0)
      fromJsonString(messageV2, 2).value shouldBe expectedV2Deserialization

      // Round trip serialization
      JsonMessage
        .fromTrustedJsonString(expectedV2Deserialization.toJsonString(basePV + 3))
        .value shouldBe expectedV2Deserialization
    }
  }
}

object HasVersionedJsonWrapperTest {

  private val basePV = ProtocolVersion.minimum

  implicit class RichProtocolVersion(val pv: ProtocolVersion) {
    def +(i: Int): ProtocolVersion = ProtocolVersion(pv.v + i)
  }

  final case class JsonMessage(
      msg: String,
      iValue: Int,
      dValue: Double,
  ) extends HasVersionedJsonWrapper[JsonMessage] {

    @transient override protected lazy val companionObj: JsonMessage.type = JsonMessage

    def toProtoV0 = VersionedMessageV0(msg)
    def toProtoV1 = VersionedMessageV1(msg, iValue)
    def toProtoV2 = VersionedMessageV2(msg, iValue, dValue)
  }

  object JsonMessage extends HasVersionedJsonMessageCompanion[JsonMessage] {
    def name: String = "Message"

    override def supportedProtoVersions: SupportedProtoVersions =
      SupportedProtoVersions(
        ProtoVersion(1) -> ProtoCodec(
          ProtocolVersion.createAlpha((basePV + 2).v),
          supportedProtoVersion(fromProtoV1),
          _.toProtoV1,
        ),
        ProtoVersion(0) -> ProtoCodec(
          ProtocolVersion.createAlpha(basePV.v),
          supportedProtoVersion(fromProtoV0),
          _.toProtoV0,
        ),
        ProtoVersion(2) -> ProtoCodec(
          ProtocolVersion.createAlpha((basePV + 3).v),
          supportedProtoVersion(fromProtoV2),
          _.toProtoV2,
        ),
      )

    def fromProtoV0(message: VersionedMessageV0): ParsingResult[JsonMessage] =
      JsonMessage(message.msg, 0, 0).asRight

    def fromProtoV1(message: VersionedMessageV1): ParsingResult[JsonMessage] =
      JsonMessage(message.msg, message.value, 1).asRight

    def fromProtoV2(message: VersionedMessageV2): ParsingResult[JsonMessage] =
      JsonMessage(message.msg, message.iValue, message.dValue).asRight
  }
}
