// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.crypto.X509CertificatePem
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.sequencing.client.http.HttpSequencerEndpoints
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import java.net.URL

class SequencerConnectionTest extends AnyWordSpec with BaseTest {
  import SequencerConnectionTest._

  "SequencerConnection.merge" should {
    "merge grpc connection endpoints" in {
      SequencerConnection.merge(Seq(grpc1, grpc2)) shouldBe Right(grpcMerged)
    }
    "fail to merge http and grpc connections" in {
      SequencerConnection.merge(Seq(grpc1, http)) shouldBe Left(
        "Cannot merge grpc and http sequencer connections"
      )
    }
    "fail with empty connections" in {
      SequencerConnection.merge(Seq.empty) shouldBe Left(
        "There must be at least one sequencer connection defined"
      )
    }
    "fail to merge http connections" in {
      SequencerConnection.merge(Seq(http, http)) shouldBe Left(
        "http connection currently only supports one endpoint"
      )
    }
    "http connection alone remains unchanged" in {
      SequencerConnection.merge(Seq(http)) shouldBe Right(http)
    }
    "grpc connection alone remains unchanged" in {
      SequencerConnection.merge(Seq(grpc1)) shouldBe Right(grpc1)
    }
  }
}

object SequencerConnectionTest {
  def endpoint(n: Int) = Endpoint(s"host$n", Port.tryCreate(100 * n))

  val grpc1 = GrpcSequencerConnection(
    NonEmpty(Seq, endpoint(1), endpoint(2)),
    false,
    Some(ByteString.copyFromUtf8("certificates")),
  )
  val grpc2 = GrpcSequencerConnection(NonEmpty(Seq, endpoint(3), endpoint(4)), false, None)
  val grpcMerged = GrpcSequencerConnection(
    NonEmpty(Seq, endpoint(1), endpoint(2), endpoint(3), endpoint(4)),
    false,
    Some(ByteString.copyFromUtf8("certificates")),
  )

  val http = HttpSequencerConnection(
    HttpSequencerEndpoints(new URL("https://host:123"), new URL("https://host:123")),
    X509CertificatePem.tryFromString("certificate"),
  )
}
