// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.config
import com.digitalasset.canton.connection.v30.{ApiInfoServiceGrpc, GetApiInfoRequest}
import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.lifecycle.HasRunOnClosing
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule, TracedLogger}
import com.digitalasset.canton.networking.grpc.{
  CantonGrpcUtil,
  GrpcError,
  ManagedChannelBuilderProxy,
}
import com.digitalasset.canton.topology.admin.v30.{
  ListPartiesRequest,
  TopologyAggregationServiceGrpc,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.AbstractStub
import io.grpc.{Channel, Status}
import org.scalatest.Assertion
import org.slf4j.event.Level.WARN

import scala.concurrent.{ExecutionContext, Future}

/** Calls the admin API of a node with a bearer token and asserts on the outcome.
  *
  * The assertion helpers live next to the calls because they expect the specific outcome of an
  * admin API call with a rejected token: the UNAUTHENTICATED status and the warnings logged by the
  * node, which are checked by both suites that mix in this trait.
  */
trait AdminApiAuthTestHelpers {
  this: CommunityIntegrationTest with HasRunOnClosing =>

  def checkCantonApiInfo(
      serverName: String,
      expectedName: String,
      channelBuilder: ManagedChannelBuilderProxy,
      logger: TracedLogger,
      timeout: config.NonNegativeDuration,
      token: Option[String],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, GrpcError, Unit] =
    sendAdminApiRequest(
      serverName,
      expectedName,
      channelBuilder,
      logger,
      timeout,
      token,
      "GetApiInfo",
      ApiInfoServiceGrpc.stub,
    )(_.getApiInfo(GetApiInfoRequest()))

  def checkListParties(
      serverName: String,
      expectedName: String,
      channelBuilder: ManagedChannelBuilderProxy,
      logger: TracedLogger,
      timeout: config.NonNegativeDuration,
      token: Option[String],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, GrpcError, Unit] =
    sendAdminApiRequest(
      serverName,
      expectedName,
      channelBuilder,
      logger,
      timeout,
      token,
      "ListParties",
      TopologyAggregationServiceGrpc.stub,
    )(
      _.listParties(
        ListPartiesRequest(
          synchronizerIds = List(),
          filterParty = "",
          filterParticipant = "",
          asOf = None,
          limit = 1,
        )
      )
    )

  private def sendAdminApiRequest[Svc <: AbstractStub[Svc], Res](
      serverName: String,
      expectedName: String,
      channelBuilder: ManagedChannelBuilderProxy,
      logger: TracedLogger,
      timeout: config.NonNegativeDuration,
      token: Option[String],
      requestDescription: String,
      stubFactory: Channel => Svc,
  )(send: Svc => Future[Res])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, GrpcError, Unit] =
    CantonGrpcUtil
      .sendSingleGrpcRequest(
        serverName = s"$serverName/$expectedName",
        requestDescription = requestDescription,
        channelBuilder = channelBuilder,
        stubFactory = stubFactory,
        timeout = timeout.unwrap,
        logger = logger,
        logPolicy = CantonGrpcUtil.SilentLogPolicy,
        hasRunOnClosing = this,
        retryPolicy = CantonGrpcUtil.RetryPolicy.noRetry,
        token = token,
      )(send)
      .failOnShutdown
      .void

  /** Expects `f` to fail as unauthenticated and checks the warnings logged meanwhile one by one. */
  def assertLogs(
      f: => EitherT[Future, GrpcError, Unit],
      assertions: (LogEntry => Assertion)*
  ): Assertion =
    loggerFactory.assertLogs(SuppressionRule.LevelAndAbove(WARN))(
      expectUnauthenticated(f),
      assertions *,
    )

  /** Expects `f` to fail as unauthenticated and checks the warnings logged meanwhile. */
  def assertUnauthenticated(
      f: => EitherT[Future, GrpcError, Unit],
      logAssertion: Seq[LogEntry] => Assertion = entries => {
        assert(entries.nonEmpty)
        forEvery(entries)(_.warningMessage should include("UNAUTHENTICATED"))
      },
  ): Assertion =
    loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(WARN))(
      expectUnauthenticated(f),
      logAssertion,
    )

  // Compares against the status code rather than a constructed MissingJwtToken error: a Canton
  // error logs a warning on creation, which an enclosing assertLogs would count as an entry.
  private def expectUnauthenticated(f: => EitherT[Future, GrpcError, Unit]): Assertion =
    inside(f.value.futureValue) {
      case Left(GrpcError.GrpcClientError(_, _, status, _, _)) =>
        status.getCode shouldBe Status.Code.UNAUTHENTICATED
      case Left(_) => fail("unexpected error")
    }
}
