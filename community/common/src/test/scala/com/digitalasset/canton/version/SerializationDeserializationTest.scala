// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.CommonMetadata
import com.digitalasset.canton.protocol.messages.AcsCommitment
import com.digitalasset.canton.protocol.{DynamicDomainParameters, StaticDomainParameters}
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  AggregationRule,
  ClosedEnvelope,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.ByteString
import org.scalacheck.Arbitrary
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
      def test[T <: HasProtocolVersionedWrapper[
        T
      ], DeserializedValueClass <: HasRepresentativeProtocolVersion](
          companion: HasProtocolVersionedWrapperWithoutContextCompanion[T, DeserializedValueClass]
      )(implicit arb: Arbitrary[T]) = testCommon(companion, companion.fromByteString)

      def testWithContext[T <: HasProtocolVersionedWrapper[
        T
      ], DeserializedValueClass <: HasRepresentativeProtocolVersion, Context](
          companion: HasMemoizedProtocolVersionedWithContextCompanion[T, Context],
          context: Context,
      )(implicit arb: Arbitrary[T]) =
        testCommon(companion, companion.fromByteString(context))

      def testCommon[T <: HasProtocolVersionedWrapper[
        T
      ], DeserializedValueClass <: HasRepresentativeProtocolVersion, Context](
          companion: HasProtocolVersionedWrapperCompanion[T, DeserializedValueClass],
          deserializer: ByteString => ParsingResult[DeserializedValueClass],
      )(implicit arb: Arbitrary[T]) = {
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

      test(StaticDomainParameters)
      test(DynamicDomainParameters)
      test(AcknowledgeRequest)
      test(AggregationRule)
      test(AcsCommitment)
      test(ClosedEnvelope)
      testWithContext(CommonMetadata, TestHash)
    }
  }
}
