// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import cats.syntax.functor.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.{OtherError, UnknownProtoVersion}
import com.digitalasset.canton.protobuf.{VersionedMessageV0, VersionedMessageV1, VersionedMessageV2}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.TopologyTransaction
import com.digitalasset.canton.version.ProtocolVersion.ProtocolVersionWithStatus
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.annotation.{nowarn, unused}

/*
 Supposing that basePV is 30, we get the scheme

  proto               0           1     2
  protocolVersion     30    31    32    33    34  ...
 */
final class HasProtocolVersionedWrapperTest extends AnyWordSpec with BaseTest {

  import HasProtocolVersionedWrapperTest.*
  "HasProtocolVersionedWrapper" should {
    "use correct proto version depending on the protocol version for serialization" in {
      def message(pv: ProtocolVersion): Message =
        Message("Hey", 1, 2.0)(protocolVersionRepresentative(pv), None)
      message(basePV).toProtoVersioned.version shouldBe 0
      message(basePV + 1).toProtoVersioned.version shouldBe 0
      message(basePV + 2).toProtoVersioned.version shouldBe 1
      message(basePV + 3).toProtoVersioned.version shouldBe 2
      message(basePV + 4).toProtoVersioned.version shouldBe 2
    }

    def fromByteString(
        bytes: ByteString,
        protoVersion: Int,
        expectedProtocolVersion: ProtocolVersion,
    ): ParsingResult[Message] = Message
      .fromByteString(
        expectedProtocolVersion,
        VersionedMessage[Message](bytes, protoVersion).toByteString,
      )

    "set correct protocol version depending on the proto version" in {

      val messageV1 = VersionedMessageV1("Hey", 42).toByteString
      val expectedV1Deserialization =
        Message("Hey", 42, 1.0)(protocolVersionRepresentative(basePV + 2), None)
      fromByteString(messageV1, 1, basePV + 2).value shouldBe expectedV1Deserialization

      // Round trip serialization
      Message
        .fromByteString(basePV + 2, expectedV1Deserialization.toByteString)
        .value shouldBe expectedV1Deserialization

      val messageV2 = VersionedMessageV2("Hey", 42, 43.0).toByteString
      val expectedV2Deserialization =
        Message("Hey", 42, 43.0)(protocolVersionRepresentative(basePV + 3), None)
      fromByteString(messageV2, 2, basePV + 3).value shouldBe expectedV2Deserialization

      // Round trip serialization
      Message
        .fromByteString(basePV + 3, expectedV2Deserialization.toByteString)
        .value shouldBe expectedV2Deserialization
    }

    "return the protocol representative" in {
      protocolVersionRepresentative(basePV + 0).representative shouldBe basePV
      protocolVersionRepresentative(basePV + 1).representative shouldBe basePV
      protocolVersionRepresentative(basePV + 2).representative shouldBe basePV + 2
      protocolVersionRepresentative(basePV + 3).representative shouldBe basePV + 3
      protocolVersionRepresentative(basePV + 4).representative shouldBe basePV + 3
      protocolVersionRepresentative(basePV + 5).representative shouldBe basePV + 3
    }

    "return the highest inclusive protocol representative for an unknown protocol version" in {
      protocolVersionRepresentative(ProtocolVersion(-1)).representative shouldBe basePV + 3
    }

    "rpv computation fails for an unknown proto version" in {
      val maxProtoVersion = Message.versioningTable.table.keys.max.v
      val unknownProtoVersion = ProtoVersion(maxProtoVersion + 1)

      Message
        .protocolVersionRepresentativeFor(unknownProtoVersion)
        .left
        .value shouldBe UnknownProtoVersion(unknownProtoVersion, Message.name)
    }

    "fail deserialization when the representative protocol version from the proto version does not match the expected (representative) protocol version" in {
      val message = VersionedMessageV1("Hey", 42).toByteString
      fromByteString(message, 1, basePV + 3).left.value should have message
        Message.unexpectedProtoVersionError(basePV + 3, basePV + 2).message
    }

    "validate proto version against expected (representative) protocol version" in {
      Message
        .validateDeserialization(ProtocolVersionValidation(basePV + 2), basePV + 2)
        .value shouldBe ()
      Message
        .validateDeserialization(ProtocolVersionValidation(basePV + 3), basePV + 2)
        .left
        .value should have message Message
        .unexpectedProtoVersionError(basePV + 3, basePV + 2)
        .message
      Message
        .validateDeserialization(
          ProtocolVersionValidation.NoValidation,
          basePV,
        )
        .value shouldBe ()
    }

    "status consistency between protobuf messages and protocol versions" in {
      new VersioningCompanionMemoization[Message] {

        // Used by the compiled string below
        @unused
        val stablePV: ProtocolVersionWithStatus[ProtocolVersionAnnotation.Stable] =
          ProtocolVersion.createStable(10)
        @unused
        val alphaPV: ProtocolVersionWithStatus[ProtocolVersionAnnotation.Alpha] =
          ProtocolVersion.createAlpha(11)

        def name: String = "message"

        override def versioningTable: VersioningTable = ???

        @unused
        private def createVersionedProtoCodec[
            ProtoClass <: scalapb.GeneratedMessage,
            Status <: ProtocolVersionAnnotation.Status,
        ](
            protoCompanion: scalapb.GeneratedMessageCompanion[ProtoClass] & Status,
            pv: ProtocolVersion.ProtocolVersionWithStatus[Status],
            deserializer: ProtoClass => DataByteString => ParsingResult[Message],
            serializer: Message => ProtoClass,
        ) =
          VersionedProtoCodec.apply[
            Message,
            Unit,
            Message,
            Message.type,
            ProtoClass,
            Status,
          ](pv)(
            protoCompanion
          )(
            supportedProtoVersionMemoized(_)(deserializer),
            serializer,
          )

        clue("can use a stable proto message in a stable protocol version") {
          assertCompiles(
            """
             createVersionedProtoCodec(
                VersionedMessageV1,
                stablePV,
                Message.fromProtoV1,
                _.toProtoV1,
              )"""
          ): Assertion
        }

        clue("can use a stable proto message in an alpha protocol version") {
          assertCompiles(
            """
             createVersionedProtoCodec(
                VersionedMessageV1,
                alphaPV,
                Message.fromProtoV1,
                _.toProtoV1,
              )"""
          ): Assertion
        }

        clue("can use an alpha proto message in an alpha protocol version") {
          assertCompiles(
            """
             createVersionedProtoCodec(
                VersionedMessageV2,
                alphaPV,
                Message.fromProtoV2,
                _.toProtoV2,
              )"""
          ): Assertion
        }

        clue("can not use an alpha proto message in a stable protocol version") {
          assertTypeError(
            """
             createVersionedProtoCodec(
                VersionedMessageV2,
                stablePV,
                Message.fromProtoV2,
                _.toProtoV2,
              )"""
          ): Assertion
        }
      }
    }
  }

  "deserializerFor" should {
    def assertNoDeserializer(
        result: ParsingResult[?],
        protoVersion: Int,
        name: String,
    ): Assertion =
      inside(result.left.value) { case OtherError(error) =>
        error should include(s"version ${ProtoVersion(protoVersion)}")
        error should include(name)
      }

    "return the deserializer of a known proto version" in {
      val bytes = VersionedMessageV1("Hey", 42).toByteString

      val deserializer = Message.versioningTable.deserializerFor(ProtoVersion(1)).value
      deserializer(ProtocolVersionValidation.NoValidation, (), bytes, bytes).value shouldBe
        Message("Hey", 42, 1.0)(protocolVersionRepresentative(basePV + 2), None)
    }

    "fail for a proto version above the highest supported one" in {
      val unknownProtoVersion = Message.versioningTable.table.keys.max.v + 1

      assertNoDeserializer(
        Message.versioningTable.deserializerFor(ProtoVersion(unknownProtoVersion)),
        unknownProtoVersion,
        Message.name,
      )
    }

    "fail for a proto version below the lowest supported one" in {
      val unknownProtoVersion = Message.versioningTable.table.keys.min.v - 1

      assertNoDeserializer(
        Message.versioningTable.deserializerFor(ProtoVersion(unknownProtoVersion)),
        unknownProtoVersion,
        Message.name,
      )
    }

    "fail for an unknown proto version in all deserialization entry points" in {
      val unknownProtoVersion = Message.versioningTable.table.keys.max.v + 1
      val bytes =
        VersionedMessage[Message](VersionedMessageV1("Hey", 42).toByteString, unknownProtoVersion)

      def check(result: ParsingResult[Message]): Assertion =
        assertNoDeserializer(result, unknownProtoVersion, Message.name)

      check(Message.fromByteString(basePV + 2, bytes.toByteString))
      check(Message.fromTrustedByteString(bytes.toByteString))
      check(Message.fromTrustedByteArray(bytes.toByteArray))

      val output = new ByteArrayOutputStream()
      bytes.writeDelimitedTo(output)
      check(
        Message.parseDelimitedFromTrusted(new ByteArrayInputStream(output.toByteArray)).value
      )
    }

    "succeed for a known proto version in all deserialization entry points" in {
      val message = Message("Hey", 42, 1.0)(protocolVersionRepresentative(basePV + 2), None)
      val bytes = message.toByteString

      Message.fromByteString(basePV + 2, bytes).value shouldBe message
      Message.fromTrustedByteString(bytes).value shouldBe message
      Message.fromTrustedByteArray(bytes.toByteArray).value shouldBe message

      val output = new ByteArrayOutputStream()
      message.writeDelimitedTo(output).value shouldBe ()
      Message
        .parseDelimitedFromTrusted(new ByteArrayInputStream(output.toByteArray))
        .value
        .value shouldBe message
    }

    "only accept proto versions below the lowest supported one for topology transactions" in {
      clue("both companions use the same versioning table") {
        TopologyLikeMessage.versioningTable.table.fmap(_.representative) shouldBe
          NonTopologyMessage.versioningTable.table.fmap(_.representative)
      }

      // There is a topology transaction with proto version 29 on MainNet
      Seq(0, 29).foreach { protoVersion =>
        val bytes = VersionedMessage[TopologyLikeMessage](
          VersionedMessageV1("Hey", 42).toByteString,
          protoVersion,
        ).toByteString

        clue(s"topology transaction with proto version $protoVersion") {
          // Legacy topology transactions are read with the lowest supported proto version
          val deserialized = TopologyLikeMessage.fromTrustedByteString(bytes).value
          deserialized.decodedWith shouldBe 30
          deserialized.representativeProtocolVersion.representative shouldBe basePV
        }

        clue(s"other message with proto version $protoVersion") {
          assertNoDeserializer(
            NonTopologyMessage.fromTrustedByteString(bytes),
            protoVersion,
            NonTopologyMessage.name,
          )
        }
      }
    }

    "fail for topology transactions with a proto version above the lowest supported one" in {
      // 31 is a gap between the supported proto versions 30 and 32
      Seq(31, 33).foreach { protoVersion =>
        clue(s"proto version $protoVersion") {
          assertNoDeserializer(
            TopologyLikeMessage.versioningTable.deserializerFor(ProtoVersion(protoVersion)),
            protoVersion,
            TopologyLikeMessage.name,
          )
        }
      }
    }
  }

  "HasProtocolVersionWrapperE" should {
    "return an error when serialization is not possible" in {
      def message(pv: ProtocolVersion): MessageE =
        MessageE("Hey", 1, 2.0)(MessageE.protocolVersionRepresentativeFor(pv), None)

      message(basePV).toProtoVersioned.value.version shouldBe 0
      message(basePV + 1).toProtoVersioned.value.version shouldBe 0
      message(basePV + 2).toProtoVersioned.value.version shouldBe 1
      message(basePV + 3).toProtoVersioned.left.value shouldBe "Nope"
      message(basePV + 4).toProtoVersioned.left.value shouldBe "Nope"
    }
  }
}

object HasProtocolVersionedWrapperTest {
  import org.scalatest.EitherValues.*

  private val basePV = ProtocolVersion.minimum

  implicit class RichProtocolVersion(val pv: ProtocolVersion) {
    def +(i: Int): ProtocolVersion = ProtocolVersion(pv.v + i)
  }

  private def protocolVersionRepresentative(
      pv: ProtocolVersion
  ): RepresentativeProtocolVersion[Message.type] =
    Message.protocolVersionRepresentativeFor(pv)

  final case class Message(
      msg: String,
      iValue: Int,
      dValue: Double,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[Message.type],
      val deserializedFrom: Option[ByteString] = None,
  ) extends HasProtocolVersionedWrapper[Message] {

    @transient override protected lazy val companionObj: Message.type = Message

    def toProtoV0 = VersionedMessageV0(msg)
    def toProtoV1 = VersionedMessageV1(msg, iValue)
    def toProtoV2 = VersionedMessageV2(msg, iValue, dValue)
  }

  object Message
      extends VersioningCompanionMemoization[Message]
      with IgnoreInSerializationTestExhaustivenessCheck {
    def name: String = "Message"

    override val versioningTable: VersioningTable = VersioningTable(
      ProtoVersion(1) -> VersionedProtoCodec(ProtocolVersion.createAlpha((basePV + 2).v))(
        VersionedMessageV1
      )(
        supportedProtoVersionMemoized(_)(fromProtoV1),
        _.toProtoV1,
      ),
      // Can use a stable Protobuf message in a stable protocol version
      ProtoVersion(0) -> VersionedProtoCodec(ProtocolVersion.createStable(basePV.v))(
        VersionedMessageV0
      )(
        supportedProtoVersionMemoized(_)(fromProtoV0),
        _.toProtoV0,
      ),
      // Can use an alpha Protobuf message in an alpha protocol version
      ProtoVersion(2) -> VersionedProtoCodec(
        ProtocolVersion.createAlpha((basePV + 3).v)
      )(VersionedMessageV2)(
        supportedProtoVersionMemoized(_)(fromProtoV2),
        _.toProtoV2,
      ),
    )

    def fromProtoV0(message: VersionedMessageV0)(bytes: ByteString): ParsingResult[Message] =
      Message(
        message.msg,
        0,
        0,
      )(
        protocolVersionRepresentativeFor(ProtoVersion(0)).value,
        Some(bytes),
      ).asRight

    def fromProtoV1(message: VersionedMessageV1)(bytes: ByteString): ParsingResult[Message] =
      Message(
        message.msg,
        message.value,
        1,
      )(
        protocolVersionRepresentativeFor(ProtoVersion(1)).value,
        Some(bytes),
      ).asRight

    def fromProtoV2(message: VersionedMessageV2)(bytes: ByteString): ParsingResult[Message] =
      Message(
        message.msg,
        message.iValue,
        message.dValue,
      )(
        protocolVersionRepresentativeFor(ProtoVersion(2)).value,
        Some(bytes),
      ).asRight
  }

  final case class MessageE(
      msg: String,
      iValue: Int,
      dValue: Double,
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[MessageE.type],
      val deserializedFrom: Option[ByteString] = None,
  ) extends HasProtocolVersionedWrapperE[MessageE] {

    @transient override protected lazy val companionObj: MessageE.type = MessageE

    def toProtoV0: Either[String, VersionedMessageV0] = VersionedMessageV0(msg).asRight
    def toProtoV1: Either[String, VersionedMessageV1] = VersionedMessageV1(msg, iValue).asRight
    def toProtoV2: Either[String, VersionedMessageV2] = "Nope".asLeft
  }

  object MessageE
      extends VersioningCompanionMemoizationE[MessageE]
      with IgnoreInSerializationTestExhaustivenessCheck {
    def name: String = "Message"

    override val versioningTable: VersioningTable = VersioningTable(
      ProtoVersion(1) -> VersionedProtoCodec.applyE(ProtocolVersion.createAlpha((basePV + 2).v))(
        VersionedMessageV1
      )(
        supportedProtoVersionMemoized(_)(fromProtoV1),
        _.toProtoV1,
      ),
      // Can use a stable Protobuf message in a stable protocol version
      ProtoVersion(0) -> VersionedProtoCodec.applyE(ProtocolVersion.createStable(basePV.v))(
        VersionedMessageV0
      )(
        supportedProtoVersionMemoized(_)(fromProtoV0),
        _.toProtoV0,
      ),
      // Can use an alpha Protobuf message in an alpha protocol version
      ProtoVersion(2) -> VersionedProtoCodec.applyE(
        ProtocolVersion.createAlpha((basePV + 3).v)
      )(VersionedMessageV2)(
        supportedProtoVersionMemoized(_)(fromProtoV2),
        _.toProtoV2,
      ),
    )

    def fromProtoV0(message: VersionedMessageV0)(bytes: ByteString): ParsingResult[MessageE] =
      MessageE(message.msg, 0, 0)(
        protocolVersionRepresentativeFor(ProtoVersion(0)).value,
        Some(bytes),
      ).asRight

    def fromProtoV1(message: VersionedMessageV1)(bytes: ByteString): ParsingResult[MessageE] =
      MessageE(message.msg, message.value, 1)(
        protocolVersionRepresentativeFor(ProtoVersion(1)).value,
        Some(bytes),
      ).asRight

    @nowarn("msg=parameter .* in method .* is never used")
    def fromProtoV2(message: VersionedMessageV2)(bytes: ByteString): ParsingResult[MessageE] =
      OtherError("No deserialization from v2").asLeft
  }

  /*
  TopologyLikeMessage and NonTopologyMessage share the same versioning table and only differ by
  their name, which is what enables the fallback for legacy topology transactions.
  `decodedWith` records the proto version of the deserializer that was used.
   proto               30          32
   protocolVersion     30    31    32    ...
   */
  final case class TopologyLikeMessage(msg: String, decodedWith: Int)(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        TopologyLikeMessage.type
      ]
  ) extends HasProtocolVersionedWrapper[TopologyLikeMessage] {

    @transient override protected lazy val companionObj: TopologyLikeMessage.type =
      TopologyLikeMessage

    def toProtoV30 = VersionedMessageV1(msg, 0)
    def toProtoV32 = VersionedMessageV2(msg, 0, 0)
  }

  object TopologyLikeMessage
      extends VersioningCompanionMemoization[TopologyLikeMessage]
      with IgnoreInSerializationTestExhaustivenessCheck {
    def name: String = TopologyTransaction.name

    override val versioningTable: VersioningTable = VersioningTable(
      ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.createAlpha((basePV + 2).v))(
        VersionedMessageV2
      )(
        supportedProtoVersionMemoized(_)(fromProtoV32),
        _.toProtoV32,
      ),
      ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.createStable(basePV.v))(
        VersionedMessageV1
      )(
        supportedProtoVersionMemoized(_)(fromProtoV30),
        _.toProtoV30,
      ),
    )

    def fromProtoV30(
        message: VersionedMessageV1
    ): ByteString => ParsingResult[TopologyLikeMessage] =
      _ =>
        protocolVersionRepresentativeFor(ProtoVersion(30))
          .map(TopologyLikeMessage(message.msg, 30)(_))

    def fromProtoV32(
        message: VersionedMessageV2
    ): ByteString => ParsingResult[TopologyLikeMessage] =
      _ =>
        protocolVersionRepresentativeFor(ProtoVersion(32))
          .map(TopologyLikeMessage(message.msg, 32)(_))
  }

  final case class NonTopologyMessage(msg: String, decodedWith: Int)(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        NonTopologyMessage.type
      ]
  ) extends HasProtocolVersionedWrapper[NonTopologyMessage] {

    @transient override protected lazy val companionObj: NonTopologyMessage.type =
      NonTopologyMessage

    def toProtoV30 = VersionedMessageV1(msg, 0)
    def toProtoV32 = VersionedMessageV2(msg, 0, 0)
  }

  object NonTopologyMessage
      extends VersioningCompanionMemoization[NonTopologyMessage]
      with IgnoreInSerializationTestExhaustivenessCheck {
    def name: String = "NonTopologyMessage"

    override val versioningTable: VersioningTable = VersioningTable(
      ProtoVersion(32) -> VersionedProtoCodec(ProtocolVersion.createAlpha((basePV + 2).v))(
        VersionedMessageV2
      )(
        supportedProtoVersionMemoized(_)(fromProtoV32),
        _.toProtoV32,
      ),
      ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.createStable(basePV.v))(
        VersionedMessageV1
      )(
        supportedProtoVersionMemoized(_)(fromProtoV30),
        _.toProtoV30,
      ),
    )

    def fromProtoV30(message: VersionedMessageV1): ByteString => ParsingResult[NonTopologyMessage] =
      _ =>
        protocolVersionRepresentativeFor(ProtoVersion(30))
          .map(NonTopologyMessage(message.msg, 30)(_))

    def fromProtoV32(message: VersionedMessageV2): ByteString => ParsingResult[NonTopologyMessage] =
      _ =>
        protocolVersionRepresentativeFor(ProtoVersion(32))
          .map(NonTopologyMessage(message.msg, 32)(_))
  }
}
