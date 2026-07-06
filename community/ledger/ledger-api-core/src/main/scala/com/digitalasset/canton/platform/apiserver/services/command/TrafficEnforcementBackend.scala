// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.TrafficAccountValidationFailed
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig
import com.digitalasset.canton.tea.v1.GetAccountRequest
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.ExecutionContext

/** Service used for enforcing user-level traffic limits on the participant node, as part of
  * submission requests in the Phase 1 of the Canton protocol.
  *
  * @param trafficServiceClient
  *   The traffic service client used to communicate with the traffic enforcement server.
  */
class TrafficEnforcementBackend(
    val trafficServiceClient: RichTrafficServiceClient,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  /** Validates that the account associated with the given account ID has sufficient balance to
    * cover the specified traffic cost.
    *
    * @param accountId
    *   The account ID a request is expected to debit traffic cost from
    * @param trafficCost
    *   The expected traffic cost of the submission request
    * @return
    *   A failed future if the account has insufficient balance or if the request to the traffic
    *   service fails, otherwise a successful future
    */
  def validateTraffic(
      accountId: String,
      trafficCost: Long,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    for {
      accountResponse <- trafficServiceClient.getAccount(GetAccountRequest(accountId))
      _ <-
        if (accountResponse.balance >= trafficCost) FutureUnlessShutdown.pure(())
        else
          FutureUnlessShutdown.failed(
            TrafficAccountValidationFailed
              .Reject(
                s"Insufficient balance (${accountResponse.balance}) for actual traffic cost ($trafficCost) for account $accountId"
              )
              .asGrpcError
          )
    } yield ()

  /** Validates that the account associated with the given actAs parties has sufficient balance to
    * cover the specified traffic cost.
    *
    * @param actAs
    *   The command's actAs parties, which should contain exactly one party for traffic enforcement,
    *   otherwise the request is rejected
    * @param trafficCost
    *   The expected traffic cost of the submission request
    * @return
    *   A failed future if the account has insufficient balance, if the actAs parties are not
    *   exactly one, or if the request to the traffic service fails, otherwise a successful future
    */
  def validateTraffic(
      actAs: Seq[LfPartyId],
      trafficCost: Long,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(
      s"Validating traffic enforcement for actAs parties: $actAs, trafficCost: $trafficCost"
    )
    for {
      singletonActAs <-
        actAs match {
          case hd :: Nil => FutureUnlessShutdown.pure(hd)
          case nonSingletonActAs =>
            FutureUnlessShutdown.failed(
              TrafficAccountValidationFailed
                .Reject(
                  show"Traffic enforcement requires exactly one actAs party for a submission. Got instead $nonSingletonActAs"
                )
                .asGrpcError
            )
        }

      // In Canton 3.5, the account ID is bound to the submitter party
      accountId = singletonActAs
      _ <- validateTraffic(accountId, trafficCost)
    } yield ()
  }

  override def onClosed(): Unit =
    LifeCycle.close(trafficServiceClient)(logger)
}

object TrafficEnforcementBackend {
  def apply(
      trafficEnforcementServerConfig: TrafficEnforcementServerConfig,
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextIdlenessExecutorService
  ): TrafficEnforcementBackend = {
    val trafficServiceClient = trafficEnforcementServerConfig match {
      case TrafficEnforcementServerConfig.Internal(inProcessTeaServerName, _projection) =>
        RichTrafficServiceClient.toInternalServer(
          grpcChannelName = inProcessTeaServerName,
          timeout = processingTimeout,
          loggerFactory = loggerFactory,
        )
    }

    new TrafficEnforcementBackend(trafficServiceClient, processingTimeout, loggerFactory)
  }
}
