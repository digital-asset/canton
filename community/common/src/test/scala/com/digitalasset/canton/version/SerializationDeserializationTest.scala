// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.protocol.{DynamicDomainParameters, StaticDomainParameters}
import org.scalacheck.Arbitrary
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks {
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*

  "Serialization and deserialization methods" should {
    "compose to the identity" in {
      def test[T <: HasProtocolVersionedWrapper[
        T
      ], DeserializedValueClass <: HasRepresentativeProtocolVersion](
          companion: HasProtocolVersionedCompanion2[T, DeserializedValueClass]
      )(implicit arb: Arbitrary[T]) = {

        forAll { instance: T =>
          val proto = instance.toByteString

          val deserializedInstance = clue(s"Deserializing serialized ${companion.name}")(
            companion.fromByteString(proto).value
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
    }
  }
}
