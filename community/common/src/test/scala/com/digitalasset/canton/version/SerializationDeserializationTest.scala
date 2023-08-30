// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.{
  CommonMetadata,
  ParticipantMetadata,
  SubmitterMetadata,
  TransferInCommonData,
  TransferOutCommonData,
}
import com.digitalasset.canton.protocol.messages.AcsCommitment
import com.digitalasset.canton.protocol.{
  ContractMetadata,
  DynamicDomainParameters,
  SerializableContract,
  StaticDomainParameters,
}
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  AggregationRule,
  ClosedEnvelope,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.SerializationDeserializationTest.DefaultValueUntilExclusive
import com.google.protobuf.ByteString
import org.scalacheck.Arbitrary
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks {
  import com.digitalasset.canton.data.GeneratorsData.*
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.sequencing.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.protocol.messages.GeneratorsMessages.*

  "Serialization and deserialization methods" should {
    "compose to the identity" in {

      /*
       Test for classes extending `HasVersionedWrapper` (protocol version passed to the serialization method),
       without context for deserialization.
       */
      def testVersioned[T <: HasVersionedWrapper[_]](
          companion: HasVersionedMessageCompanion[T],
          defaults: List[DefaultValueUntilExclusive[T, _]] = Nil,
      )(implicit arb: Arbitrary[T]): Assertion =
        testVersionedCommon(companion, companion.fromByteString, defaults)

      /*
       Test for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
       without context for deserialization.
       */
      def testProtocolVersioned[T <: HasProtocolVersionedWrapper[
        T
      ], DeserializedValueClass <: HasRepresentativeProtocolVersion](
          companion: HasProtocolVersionedWrapperWithoutContextCompanion[T, DeserializedValueClass]
      )(implicit arb: Arbitrary[T]): Assertion =
        testProtocolVersionedCommon(companion, companion.fromByteString)

      /*
       Test for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
       with context for deserialization.
       */
      def testProtocolVersionedWithContext[T <: HasProtocolVersionedWrapper[
        T
      ], DeserializedValueClass <: HasRepresentativeProtocolVersion, Context](
          companion: HasMemoizedProtocolVersionedWithContextCompanion[T, Context],
          context: Context,
      )(implicit arb: Arbitrary[T]): Assertion =
        testProtocolVersionedCommon(companion, companion.fromByteString(context))

      testProtocolVersioned(StaticDomainParameters)
      testProtocolVersioned(DynamicDomainParameters)
      testProtocolVersioned(AcknowledgeRequest)
      testProtocolVersioned(AggregationRule)
      testProtocolVersioned(AcsCommitment)
      testProtocolVersioned(ClosedEnvelope)

      testVersioned(ContractMetadata)
      testVersioned[SerializableContract](
        SerializableContract,
        List(DefaultValueUntilExclusive(_.copy(contractSalt = None), ProtocolVersion.v4)),
      )

      // Merkle tree leaves
      testProtocolVersionedWithContext(CommonMetadata, TestHash)
      testProtocolVersionedWithContext(ParticipantMetadata, TestHash)
      testProtocolVersionedWithContext(SubmitterMetadata, TestHash)
      testProtocolVersionedWithContext(TransferInCommonData, TestHash)
      // TransferInView
      testProtocolVersionedWithContext(TransferOutCommonData, TestHash)
      // TransferOutView
      // ViewCommonData
      // ViewParticipantData

    }
  }

  /*
    Shared test code for classes extending `HasVersionedWrapper` (protocol version passed to the serialization method),
    with/without context for deserialization.
   */
  private def testVersionedCommon[T <: HasVersionedWrapper[_]](
      companion: HasVersionedMessageCompanionCommon[T],
      deserializer: ByteString => ParsingResult[_],
      defaults: List[DefaultValueUntilExclusive[T, _]],
  )(implicit arb: Arbitrary[T]): Assertion = {
    import com.digitalasset.canton.version.GeneratorsVersion.protocolVersionArb

    forAll { (instance: T, protocolVersion: ProtocolVersion) =>
      val proto = instance.toByteString(protocolVersion)

      val deserializedInstance = clue(s"Deserializing serialized ${companion.name}")(
        deserializer(proto).value
      )

      val updatedInstance = defaults.foldLeft(instance) {
        case (instance, DefaultValueUntilExclusive(transformer, untilExclusive)) =>
          if (protocolVersion < untilExclusive) transformer(instance) else instance
      }

      withClue(
        s"Comparing ${companion.name} with (de)serialization done for pv=$protocolVersion"
      ) {
        updatedInstance shouldBe deserializedInstance
      }
    }
  }

  /*
     Shared test code for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
     with/without context for deserialization.
   */
  private def testProtocolVersionedCommon[T <: HasProtocolVersionedWrapper[
    T
  ], DeserializedValueClass <: HasRepresentativeProtocolVersion, Context](
      companion: HasProtocolVersionedWrapperCompanion[T, DeserializedValueClass],
      deserializer: ByteString => ParsingResult[DeserializedValueClass],
  )(implicit arb: Arbitrary[T]): Assertion = {
    forAll { instance: T =>
      val proto = instance.toByteString

      val deserializedInstance = clue(s"Deserializing serialized ${companion.name}")(
        deserializer(proto).value
      )

      withClue(
        s"Comparing ${companion.name} with representative ${instance.representativeProtocolVersion}"
      ) {
        instance shouldBe deserializedInstance
        instance.representativeProtocolVersion shouldBe deserializedInstance.representativeProtocolVersion
      }
    }
  }
}

private object SerializationDeserializationTest {
  final case class DefaultValueUntilExclusive[ValueClass, T](
      transformer: ValueClass => ValueClass,
      untilExclusive: ProtocolVersion,
  )
}
