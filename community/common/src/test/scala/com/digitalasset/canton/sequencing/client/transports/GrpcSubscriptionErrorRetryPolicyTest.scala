// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication
import com.digitalasset.canton.sequencing.authentication.grpc.Constant
import com.digitalasset.canton.topology.DefaultTestIdentities
import io.grpc.{Metadata, Status}
import org.scalatest.wordspec.AnyWordSpec

class GrpcSubscriptionErrorRetryPolicyTest extends AnyWordSpec with BaseTest {

  private val requestDescription = "request description"
  private val serverName = "sequencer"

  "GrpcSubscriptionErrorRetryPolicy" should {
    val tokenExpiredMetadata = new Metadata()
    tokenExpiredMetadata.put(
      Constant.AUTHENTICATION_ERROR_CODE,
      MemberAuthentication.MissingToken(DefaultTestIdentities.participant1).code,
    )

    val recoverableErrors = Seq(
      GrpcError(requestDescription, serverName, Status.UNAVAILABLE.asRuntimeException()),
      GrpcError(
        requestDescription,
        serverName,
        Status.UNAUTHENTICATED.asRuntimeException(tokenExpiredMetadata),
      ),
    )

    val policy = new GrpcSubscriptionErrorRetryPolicy(loggerFactory)

    forEvery(recoverableErrors) { error =>
      s"retry if sequencer temporarily fails with ${error.status.getCode}" in {
        policy.retryOnError(GrpcSubscriptionError(error)) shouldBe true
      }
    }

    s"not retry if sequencer fails with GrpcSubscriptionUnexpectedException" in {
      val error = GrpcSubscriptionUnexpectedException(new RuntimeException("BOOM"))
      policy.retryOnError(error) shouldBe false
    }
  }
}
