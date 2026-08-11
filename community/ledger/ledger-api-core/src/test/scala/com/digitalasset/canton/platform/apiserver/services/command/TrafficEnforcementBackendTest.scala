// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.tea.v1.{GetAccountRequest, GetAccountResponse}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, LfPartyId}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

class TrafficEnforcementBackendTest
    extends AsyncWordSpec
    with MockitoSugar
    with ArgumentMatchersSugar
    with HasExecutionContext
    with FailOnShutdown
    with BaseTest {

  private val adminParty = LfPartyId.assertFromString("participantAdmin")
  private val alice = LfPartyId.assertFromString("Alice")
  private val bob = LfPartyId.assertFromString("Bob")

  private def newBackend(
      enforceCostOnSubmissions: Boolean,
      rejectMultiPartySubmissions: Boolean,
      trafficServiceClient: RichTrafficServiceClient,
  ): TrafficEnforcementBackend =
    new TrafficEnforcementBackend(
      enforceCostOnSubmissions = enforceCostOnSubmissions,
      rejectMultiPartySubmissions = rejectMultiPartySubmissions,
      trafficServiceClient = trafficServiceClient,
      adminParty = adminParty,
      timeouts = ProcessingTimeout(),
      loggerFactory = loggerFactory,
    )

  "validateTraffic" should {

    "succeed when the singleton actAs party's account has sufficient balance" in {
      val client = mock[RichTrafficServiceClient]
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(
          FutureUnlessShutdown.pure(GetAccountResponse(accountId = alice, balance = 10L))
        )
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      backend.validateTraffic(actAs = Seq(alice), trafficCost = 10L).value.map { result =>
        result shouldBe Right(())
      }
    }

    "reject when the singleton actAs party's account has insufficient balance" in {
      val client = mock[RichTrafficServiceClient]
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(FutureUnlessShutdown.pure(GetAccountResponse(accountId = alice, balance = 5L)))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      backend.validateTraffic(actAs = Seq(alice), trafficCost = 10L).value.map {
        case Left(err) =>
          err.code.id shouldBe "TRAFFIC_ACCOUNT_VALIDATION_FAILED"
          err.cause should include regex raw"Insufficient balance \(5\) for actual traffic cost \(10\) for account $alice"
        case Right(_) => fail("expected the submission to be rejected")
      }
    }

    "reject a non-singleton actAs submission when configured to, without looking up an account" in {
      val client = mock[RichTrafficServiceClient]
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      backend.validateTraffic(actAs = Seq(alice, bob), trafficCost = 10L).value.map {
        case Left(err) =>
          verifyZeroInteractions(client)
          err.code.id shouldBe "TRAFFIC_MULTI_PARTY_SUBMISSION_REJECTED"
        case Right(_) => fail("expected the submission to be rejected")
      }
    }

    "propagate a failed account lookup as a failed future rather than a rejection" in {
      val client = mock[RichTrafficServiceClient]
      val serviceFailure = new RuntimeException("traffic service unavailable")
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(FutureUnlessShutdown.failed(serviceFailure))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      backend
        .validateTraffic(actAs = Seq(alice), trafficCost = 10L)
        .value
        .unwrap
        .failed
        .map(_ shouldBe serviceFailure)
    }
  }
}
