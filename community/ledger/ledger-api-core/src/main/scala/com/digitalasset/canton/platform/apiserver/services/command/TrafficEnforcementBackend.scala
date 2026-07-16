// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
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
  * @param enforceCostOnSubmissions
  *   Whether to enforce traffic cost on submissions.
  * @param trafficServiceClient
  *   The traffic service client used to communicate with the traffic enforcement server.
  */
class TrafficEnforcementBackend(
    enforceCostOnSubmissions: Boolean,
    val trafficServiceClient: RichTrafficServiceClient,
    adminParty: LfPartyId,
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
        if (!enforceCostOnSubmissions || accountResponse.balance >= trafficCost)
          FutureUnlessShutdown.pure(())
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
    *   otherwise the traffic validation for the request is skipped and an information message is
    *   logged
    * @param trafficCost
    *   The expected traffic cost of the submission request
    * @return
    *   Success if validation is successful
    */
  def validateTraffic(
      actAs: Seq[LfPartyId],
      trafficCost: Long,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(
      s"Validating traffic enforcement for actAs parties: $actAs, trafficCost: $trafficCost"
    )

    actAs match {
      case singleActAs :: Nil if singleActAs == adminParty =>
        logger.debug(
          show"Skipping traffic enforcement validation for participant admin party: $singleActAs"
        )
        FutureUnlessShutdown.unit
      case singleActAs :: Nil =>
        validateTraffic(
          // In Canton 3.5, the account ID is bound to the submitter party
          accountId = singleActAs,
          trafficCost = trafficCost,
        )
      case nonSingletonActAs =>
        logger.info(
          show"Skipping traffic enforcement validation due to non-singleton actAs parties: $nonSingletonActAs"
        )
        FutureUnlessShutdown.unit
    }
  }

  override def onClosed(): Unit =
    LifeCycle.close(trafficServiceClient)(logger)
}

object TrafficEnforcementBackend {
  def apply(
      enforceCostOnSubmissions: Boolean,
      trafficEnforcementServerConfig: TrafficEnforcementServerConfig,
      instanceName: InstanceName,
      ledgerApiPort: Port,
      adminParty: LfPartyId,
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextIdlenessExecutorService
  ): TrafficEnforcementBackend = {
    val trafficServiceClient = trafficEnforcementServerConfig match {
      case internal: TrafficEnforcementServerConfig.Internal =>
        RichTrafficServiceClient.toInternalServer(
          grpcChannelName = internal.processServerNameForInstance(instanceName, ledgerApiPort),
          timeout = processingTimeout,
          loggerFactory = loggerFactory,
        )
    }

    new TrafficEnforcementBackend(
      enforceCostOnSubmissions,
      trafficServiceClient,
      adminParty,
      processingTimeout,
      loggerFactory,
    )
  }
}
