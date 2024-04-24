// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.admin.domain.v30
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.{BaseTest, config}
import org.scalatest.wordspec.AnyWordSpec

class SequencerConnectionsTest extends AnyWordSpec with BaseTest {

  "fromProto30" should {
    "parse the old protobuf message format" in {
      val grpcConnection = GrpcSequencerConnection.tryCreate("http://localhost:80")
      val legacy = new v30.SequencerConnections(Seq(grpcConnection.toProtoV30), 1, 2, None)
      val expected = SequencerConnections.tryMany(
        Seq(grpcConnection),
        PositiveInt.one,
        SubmissionRequestAmplification(
          PositiveInt.tryCreate(2),
          config.NonNegativeFiniteDuration.Zero,
        ),
      )
      SequencerConnections.fromProtoV30(legacy) shouldBe Right(expected)
    }
  }

}
