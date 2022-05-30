// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protobuf

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.protobuf.UntypedVersionedMessageTest.{Message, parseNew, parseOld}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

class UntypedVersionedMessageTest extends AnyWordSpec with BaseTest {

  "UntypedVersionedMessage" should {
    "be compatible with old Versioned messages" in {
      /*
        In this test, we refer to the Versioned* messages as "old" and the
        UntypedVersionedMessage as new.
        We check that old bytestring can be read with new parser and vice versa.
       */

      def test(content: String): Assertion = {
        val message: Message = Message(content)

        val newByteString = message.toProtoVersioned(ProtocolVersion.latestForTest).toByteString
        val oldByteString = VersionedDummyMessage(
          VersionedDummyMessage.Version.V0(DummyMessage(content))
        ).toByteString

        parseNew(newByteString).value shouldBe message
        parseNew(oldByteString).value shouldBe message
        parseOld(oldByteString).value shouldBe message
        parseOld(newByteString).value shouldBe message
      }

      test("Hello world!")

      // Because protobuf skip values that are equal to default value, we test for empty string
      test("")
    }
  }
}

object UntypedVersionedMessageTest {
  def parseNew(bytes: ByteString): ParsingResult[Message] = Message.fromByteString(bytes)
  def parseOld(bytes: ByteString): ParsingResult[Message] = {
    def fromProtoVersioned(dummyMessageP: VersionedDummyMessage): Either[FieldNotSet, Message] =
      dummyMessageP.version match {
        case VersionedDummyMessage.Version.Empty =>
          Left(FieldNotSet("VersionedDummyMessage.version"))
        case VersionedDummyMessage.Version.V0(parameters) => fromProtoV0(parameters)
      }

    def fromProtoV0(dummyMessageP: DummyMessage): Right[Nothing, Message] =
      Right(Message(dummyMessageP.content))

    ProtoConverter
      .protoParser(VersionedDummyMessage.parseFrom)(bytes)
      .flatMap(fromProtoVersioned)
  }

  case class Message(content: String)
      extends HasVersionedWrapper[VersionedMessage[Message]]
      with HasProtoV0[DummyMessage] {
    override def toProtoVersioned(
        version: ProtocolVersion
    ): VersionedMessage[Message] = VersionedMessage(toProtoV0.toByteString, 0)

    override def toProtoV0: DummyMessage = DummyMessage(content)
  }

  object Message extends HasVersionedMessageCompanion[Message] {
    val supportedProtoVersions: Map[Int, Parser] = Map(
      0 -> supportedProtoVersion(DummyMessage)(fromProtoV0)
    )

    val name: String = "Message"

    def fromProtoV0(messageP: DummyMessage): ParsingResult[Message] = Right(
      Message(messageP.content)
    )
  }
}
