// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.v1.GetAccountRequest
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.ExecutionContext

/** Service used for enforcing user-level traffic limits on the participant node, as part of
  * submission requests in the Phase 1 of the Canton protocol.
  *
  * @param enforceCostOnSubmissions
  *   Whether to enforce traffic cost on submissions.
  * @param rejectMultiPartySubmissions
  *   Whether to reject submissions whose actAs has more than one party (or none), instead of
  *   skipping traffic enforcement validation for them.
  * @param trafficServiceClient
  *   The traffic service client used to communicate with the traffic enforcement server.
  */
class TrafficEnforcementBackend(
    enforceCostOnSubmissions: Boolean,
    rejectMultiPartySubmissions: Boolean,
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
    *   A `Left` if the account has insufficient balance, a failed future if the request to the
    *   service fails, otherwise a successful `Right`
    */
  def validateTraffic(
      accountId: String,
      trafficCost: Long,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.InsufficientBalance.Reject, Unit] = {
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)
    for {
      accountResponse <- EitherT.right[TrafficEnforcementErrors.InsufficientBalance.Reject](
        trafficServiceClient.getAccount(GetAccountRequest(accountId))
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        !enforceCostOnSubmissions || accountResponse.balance >= trafficCost,
        (),
        TrafficEnforcementErrors.InsufficientBalance.Reject(
          s"Insufficient balance (${accountResponse.balance}) for actual traffic cost ($trafficCost) for account $accountId"
        ),
      )
    } yield ()
  }

  /** Validates that the account associated with the given actAs parties has sufficient balance to
    * cover the specified traffic cost.
    *
    * @param actAs
    *   The command's actAs parties, which should contain exactly one party for traffic enforcement.
    *   If it does not, the traffic validation for the request is either skipped (with an
    *   informational message logged) or the submission is rejected, depending on
    *   `rejectMultiPartySubmissions`.
    * @param trafficCost
    *   The expected traffic cost of the submission request
    * @return
    *   Success if validation is successful
    */
  def validateTraffic(
      actAs: Seq[LfPartyId],
      trafficCost: Long,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.TrafficEnforcementError, Unit] = {
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)
    logger.debug(
      s"Validating traffic enforcement for actAs parties: $actAs, trafficCost: $trafficCost"
    )

    actAs match {
      case singleActAs :: Nil if singleActAs == adminParty =>
        logger.debug(
          show"Skipping traffic enforcement validation for participant admin party: $singleActAs"
        )
        EitherT.pure(())
      case singleActAs :: Nil =>
        // In Canton 3.5, the account ID is bound to the submitter party
        validateTraffic(accountId = singleActAs, trafficCost = trafficCost)
          .leftWiden[TrafficEnforcementErrors.TrafficEnforcementError]
      case nonSingletonActAs if rejectMultiPartySubmissions =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          TrafficEnforcementErrors.MultiPartySubmissionRejected.Reject(
            show"Traffic enforcement rejected submission with non-singleton actAs parties: $nonSingletonActAs"
          )
        )
      case nonSingletonActAs =>
        logger.info(
          show"Skipping traffic enforcement validation due to non-singleton actAs parties: $nonSingletonActAs"
        )
        EitherT.pure(())
    }
  }

  override def onClosed(): Unit =
    LifeCycle.close(trafficServiceClient)(logger)
}

object TrafficEnforcementBackend {
  def apply(
      enforceCostOnSubmissions: Boolean,
      rejectMultiPartySubmissions: Boolean,
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
      rejectMultiPartySubmissions,
      trafficServiceClient,
      adminParty,
      processingTimeout,
      loggerFactory,
    )
  }
}
