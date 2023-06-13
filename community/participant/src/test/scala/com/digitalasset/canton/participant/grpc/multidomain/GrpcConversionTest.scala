// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import com.digitalasset.canton.{BaseTest, LfTimestamp}
import org.scalatest.wordspec.AnyWordSpec

class GrpcConversionTest extends AnyWordSpec with BaseTest {

  "GrpcConversion" should {
    // Used to populate TransferredOutEvent.transferOutId
    "convert LfTimestamp to api TransferId" in {
      val ts = LfTimestamp.now()

      val apiTransferId = GrpcConversion.toApiTransferId(ts)
      LfTimestamp.assertFromLong(apiTransferId.toLong) shouldBe ts
    }
  }
}
