// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.value.Value
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{BaseTest, LfPartyId}
import org.scalatest.wordspec.AnyWordSpec

class SerializableContractTest extends AnyWordSpec with BaseTest {

  val alice = LfPartyId.assertFromString("Alice")
  val bob = LfPartyId.assertFromString("Bob")

  val templateId = ExampleTransactionFactory.templateId

  "SerializableContractInstance" should {
    "deserialize correctly" in {
      val contractId = ExampleTransactionFactory.suffixedId(0, 0)

      val metadata = ContractMetadata.tryCreate(
        signatories = Set(alice),
        stakeholders = Set(alice, bob),
        maybeKeyWithMaintainers = Some(
          ExampleTransactionFactory.globalKeyWithMaintainers(
            LfGlobalKey(templateId, Value.ValueUnit),
            Set(alice),
          )
        ),
      )

      val sci = ExampleTransactionFactory.asSerializable(
        contractId,
        ExampleTransactionFactory.contractInstance(Seq(contractId)),
        metadata,
        CantonTimestamp.now(),
      )
      SerializableContract.fromProtoVersioned(
        sci.toProtoVersioned(testedProtocolVersion)
      ) shouldEqual Right(sci)
    }
  }
}
