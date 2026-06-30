// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.logging.{LoggingContextWithTrace, SuppressionRule}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.MethodDescriptor.Marshaller
import io.grpc.protobuf.StatusProto
import io.grpc.{Metadata, MethodDescriptor, ServerCall, Status}
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.event.Level

import scala.concurrent.{Future, Promise}

class AuthInterceptorSpec
    extends AsyncFlatSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with BaseTest
    with HasExecutionContext {

  private val className = classOf[AuthInterceptor].getSimpleName

  behavior of s"$className.interceptCall"

  private val AuthInterceptorSuppressionRule: SuppressionRule =
    SuppressionRule.forLogger[AuthInterceptor] && SuppressionRule.Level(Level.ERROR)

  it should "close the ServerCall with a V2 status code on decoding failure" in {
    loggerFactory.assertLogs(AuthInterceptorSuppressionRule)(
      within = testServerCloseError { case (actualStatus, actualMetadata) =>
        actualStatus.getCode shouldBe Status.Code.INTERNAL
        actualStatus.getDescription shouldBe "An error occurred. Please contact the operator and inquire about the request <no-correlation-id> with tid <no-tid>"

        val actualRpcStatus = StatusProto.fromStatusAndTrailers(actualStatus, actualMetadata)
        actualRpcStatus.getDetailsList.size() shouldBe 0
      },
      assertions = _.errorMessage should include(
        "INTERNAL_AUTHORIZATION_ERROR(4,0): Failed to get claims from request metadata"
      ),
    )
  }

  it should "log the deferred warnings only once when no AuthService verifies the token" in {
    val warnRule =
      SuppressionRule.forLogger[AuthInterceptor] && SuppressionRule.LevelAndAbove(Level.WARN)

    // Two services that both fail to verify a present token, each contributing a deferred warning.
    val firstService = mock[AuthService]
    val secondService = mock[AuthService]
    when(firstService.decodeToken(any[Option[String]], any[String])(anyTraceContext))
      .thenReturn(
        Future.successful(AuthService.Result(ClaimSet.Unauthenticated, Some("first failure")))
      )
    when(secondService.decodeToken(any[Option[String]], any[String])(anyTraceContext))
      .thenReturn(
        Future.successful(AuthService.Result(ClaimSet.Unauthenticated, Some("second failure")))
      )

    val authInterceptor =
      new AuthInterceptor(List(firstService, secondService), loggerFactory, executionContext)
    implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.ForTesting

    // Passing exactly one entry assertion makes assertLogs fail if more than one warning is logged,
    // so this also proves the two failures are collapsed into a single warning.
    loggerFactory.assertLogs(warnRule)(
      within = authInterceptor
        .headerToClaims(Some("Bearer some-token"), "some-service")
        .map(_ shouldBe ClaimSet.Unauthenticated),
      _.warningMessage should (fullyMatch regex ".*[(]1[)].* first failure.* [(]2[)].* second failure"),
    )
  }

  private def testServerCloseError(
      assertRpcStatus: (Status, Metadata) => Assertion
  ): Future[Assertion] = {
    val authService = mock[AuthService]
    val serverCall = mock[ServerCall[Nothing, Nothing]]
    val marshaller = mock[Marshaller[Nothing]]
    val methodDescriptor = MethodDescriptor
      .newBuilder[Nothing, Nothing](
        marshaller,
        marshaller,
      )
      .setFullMethodName("")
      .setType(MethodDescriptor.MethodType.UNARY)
      .build()
    val failedMetadataDecode =
      Future.failed[AuthService.Result](new RuntimeException("some internal failure"))

    val promise = Promise[Unit]()
    // Using a promise to ensure the verify call below happens after the expected call to `serverCall.close`
    when(serverCall.getMethodDescriptor).thenReturn(methodDescriptor)
    when(serverCall.getAttributes).thenCallRealMethod();
    when(serverCall.close(any[Status], any[Metadata])).thenAnswer {
      promise.success(())
      ()
    }

    val authInterceptor =
      new AuthInterceptor(
        List(authService),
        loggerFactory,
        executionContext,
      )

    val statusCaptor = ArgCaptor[Status]
    val metadataCaptor = ArgCaptor[Metadata]

    when(authService.decodeToken(any[Option[String]], any[String])(anyTraceContext))
      .thenReturn(failedMetadataDecode)

    new GrpcAuthInterceptor(
      authInterceptor,
      loggerFactory,
      ApiLoggingConfig(),
      executionContext,
    ).interceptCall[Nothing, Nothing](serverCall, new Metadata(), null)

    promise.future.map { _ =>
      verify(serverCall).close(statusCaptor.capture, metadataCaptor.capture)
      assertRpcStatus(statusCaptor.value, metadataCaptor.value)
    }
  }

}
