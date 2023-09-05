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
  TransferInView,
  TransferOutCommonData,
  TransferOutView,
  ViewCommonData,
}
import com.digitalasset.canton.protocol.messages.AcsCommitment
import com.digitalasset.canton.protocol.{
  ConfirmationPolicy,
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
import com.digitalasset.canton.version.SerializationDeserializationTestHelpers.DefaultValueUntilExclusive
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SerializationDeserializationTest
    extends AnyWordSpec
    with BaseTest
    with ScalaCheckPropertyChecks
    with SerializationDeserializationTestHelpers {
  import com.digitalasset.canton.data.GeneratorsData.*
  import com.digitalasset.canton.data.GeneratorsTransferData.*
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.sequencing.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.protocol.messages.GeneratorsMessages.*

  "Serialization and deserialization methods" should {
    "compose to the identity" in {

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
      testProtocolVersionedWithContext(TransferInView, TestHash)
      testProtocolVersionedWithContext(TransferOutCommonData, TestHash)
      testProtocolVersionedWithContext(TransferOutView, TestHash)

      Seq(ConfirmationPolicy.Vip, ConfirmationPolicy.Signatory).map { confirmationPolicy =>
        testProtocolVersionedWithContext(ViewCommonData, (TestHash, confirmationPolicy))
      }

      // ViewParticipantData
    }
  }
}
