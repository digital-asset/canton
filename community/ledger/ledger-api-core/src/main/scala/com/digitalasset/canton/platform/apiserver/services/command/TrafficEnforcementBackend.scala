// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import cats.Eval
import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.*
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.v1.GetAccountRequest
import com.digitalasset.canton.tracing.Spanning.SpanWrapper
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.ShowUtil.*
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext

trait TrafficEnforcementBackend {

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
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.TrafficEnforcementError, Unit]

  def trafficServiceClient: RichTrafficServiceClient
}

/** Service used for enforcing user-level traffic limits on the participant node, as part of
  * submission requests in the Phase 1 of the Canton protocol.
  *
  * @param enforceCostOnSubmissions
  *   Whether to enforce traffic cost on submissions. When disabled, no account lookup is performed
  *   at all on the submission path.
  * @param rejectMultiPartySubmissions
  *   Whether to reject submissions whose actAs has more than one party (or none), instead of
  *   skipping traffic enforcement validation for them.
  * @param allowSubmissionsOnDegradation
  *   Whether to let a submission proceed without a balance check when the account lookup fails,
  *   instead of failing the submission.
  * @param trafficServiceClient
  *   The traffic service client used to communicate with the traffic enforcement server.
  */
class TrafficEnforcementBackendImpl(
    enforceCostOnSubmissions: Boolean,
    rejectMultiPartySubmissions: Boolean,
    allowSubmissionsOnDegradation: Boolean,
    override val trafficServiceClient: RichTrafficServiceClient,
    adminParty: LfPartyId,
    metrics: TrafficEnforcementMetrics,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends TrafficEnforcementBackend
    with NamedLogging
    with FlagCloseable
    with Spanning {

  def validateTraffic(
      actAs: Seq[LfPartyId],
      trafficCost: Long,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.TrafficEnforcementError, Unit] = {
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)
    metrics.enforcementCheckDuration.timeEitherFUSWithLabels(
      withSpan(TrafficEnforcementBackend.EnforcementSpanName) { spanTraceContext => span =>
        decide(actAs, trafficCost, span)(spanTraceContext, errorLoggingContext)
      },
      labelMapping = {
        case Left(e) => e.code.id
        case Right(_) => "success"
      },
      failedStatus = TrafficEnforcementOutcome.Failed,
    )
  }

  private def decide(
      actAs: Seq[LfPartyId],
      trafficCost: Long,
      span: SpanWrapper,
  )(implicit
      traceContext: TraceContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.TrafficEnforcementError, Unit] = {
    logger.debug(
      s"Validating traffic enforcement for actAs parties: $actAs, trafficCost: $trafficCost"
    )
    actAs match {
      case singleActAs :: Nil if singleActAs == adminParty =>
        logger.debug(
          show"Skipping traffic enforcement validation for participant admin party: $singleActAs"
        )
        recordOutcome(
          span,
          TrafficEnforcementOutcome.Skipped,
          Some(TrafficEnforcementOutcome.AdminParty),
        )
        EitherT.pure(())
      case singleActAs :: Nil =>
        // In Canton 3.5, the account ID is bound to the submitter party
        validateBalance(accountId = singleActAs, trafficCost = trafficCost, span = span)
          .leftWiden[TrafficEnforcementErrors.TrafficEnforcementError]
      case nonSingletonActAs if rejectMultiPartySubmissions =>
        recordOutcome(
          span,
          TrafficEnforcementOutcome.Rejected,
          Some(TrafficEnforcementOutcome.MultiPartySubmission),
        )
        EitherT.leftT[FutureUnlessShutdown, Unit](
          TrafficEnforcementErrors.MultiPartySubmissionRejected.Reject(
            show"Traffic enforcement rejected submission with non-singleton actAs parties: $nonSingletonActAs"
          )
        )
      case nonSingletonActAs =>
        logger.info(
          show"Skipping traffic enforcement validation due to non-singleton actAs parties: $nonSingletonActAs"
        )
        recordOutcome(
          span,
          TrafficEnforcementOutcome.Skipped,
          Some(TrafficEnforcementOutcome.NonSingletonActAs),
        )
        EitherT.pure(())
    }
  }

  private def validateBalance(
      accountId: String,
      trafficCost: Long,
      span: SpanWrapper,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.InsufficientBalance.Reject, Unit] =
    if (!enforceCostOnSubmissions) {
      recordOutcome(
        span,
        TrafficEnforcementOutcome.Skipped,
        Some(TrafficEnforcementOutcome.EnforcementDisabled),
      )
      EitherT.pure[FutureUnlessShutdown, TrafficEnforcementErrors.InsufficientBalance.Reject](())
    } else {
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromTracedLogger(logger)
      for {
        accountResponseO <- EitherT.right[TrafficEnforcementErrors.InsufficientBalance.Reject](
          trafficServiceClient
            .getAccount(GetAccountRequest(accountId))
            .value
            .flatMap {
              case Right(response) => FutureUnlessShutdown.pure(Some(response))
              case Left(grpcError)
                  if TrafficEnforcementBackend.allowsSubmissionOnLookupFailure(
                    allowSubmissionsOnDegradation,
                    grpcError,
                  ) =>
                logger.warn(
                  s"Traffic enforcement account lookup failed for account $accountId; degrading" +
                    s" and allowing the submission to proceed without a balance check.\n$grpcError"
                )
                recordOutcome(
                  span,
                  TrafficEnforcementOutcome.Degraded,
                  Some(TrafficEnforcementOutcome.LookupUnavailable),
                )
                span.recordException(grpcError.asRuntimeException)
                FutureUnlessShutdown.pure(None)
              case Left(grpcError) =>
                recordOutcome(
                  span,
                  TrafficEnforcementOutcome.Failed,
                  Some(TrafficEnforcementOutcome.LookupFailed),
                )
                // The span picks up the exception and the ERROR status from closeSpan as it propagates.
                FutureUnlessShutdown.failed(
                  RichTrafficServiceClient.normalizeTeaError(grpcError)
                )
            }
        )
        _ <- accountResponseO match {
          case None =>
            EitherT.pure[FutureUnlessShutdown, TrafficEnforcementErrors.InsufficientBalance.Reject](
              ()
            )
          case Some(accountResponse) =>
            if (accountResponse.balance >= trafficCost) {
              recordOutcome(span, TrafficEnforcementOutcome.Accepted)
              EitherT.pure[
                FutureUnlessShutdown,
                TrafficEnforcementErrors.InsufficientBalance.Reject,
              ](())
            } else {
              recordOutcome(
                span,
                TrafficEnforcementOutcome.Rejected,
                Some(TrafficEnforcementOutcome.InsufficientBalance),
              )
              EitherT.leftT[FutureUnlessShutdown, Unit](
                TrafficEnforcementErrors.InsufficientBalance.Reject(
                  s"Insufficient balance (${accountResponse.balance}) for actual traffic cost ($trafficCost) for account $accountId"
                )
              )
            }
        }
      } yield ()
    }

  private def recordOutcome(
      span: SpanWrapper,
      outcome: String,
      reason: Option[String] = None,
  ): Unit = {
    span.setAttribute(TrafficEnforcementOutcome.OutcomeAttribute, outcome)
    reason.foreach(span.setAttribute(TrafficEnforcementOutcome.ReasonAttribute, _))
    metrics.markDecision(outcome, reason)
  }

  override def onClosed(): Unit =
    LifeCycle.close(trafficServiceClient)(logger)
}

object TrafficEnforcementBackend {

  private[command] val EnforcementSpanName: String = "TrafficEnforcementBackend.validateTraffic"

  /** Let the submission through only when the lookup could not produce an answer, never when the
    * traffic service produced one: a refusal is a deterministic answer, and a client cancellation
    * means there is no submission left to protect.
    */
  private[command] def allowsSubmissionOnLookupFailure(
      allowSubmissionsOnDegradation: Boolean,
      error: GrpcError,
  ): Boolean =
    allowSubmissionsOnDegradation && (error match {
      case _: GrpcError.GrpcServiceUnavailable => true
      case _: GrpcError.GrpcServerError => true
      case gaveUp: GrpcError.GrpcClientGaveUp => !gaveUp.isClientCancellation
      case _: GrpcError.GrpcClientError => false
      case _: GrpcError.GrpcRequestRefusedByServer => false
      case _: GrpcError.GrpcRequestRefusedAlreadyExists => false
    })

  def apply(
      enforceCostOnSubmissions: Boolean,
      rejectMultiPartySubmissions: Boolean,
      allowSubmissionsOnDegradation: Boolean,
      trafficEnforcementServerConfig: TrafficEnforcementServerConfig,
      instanceName: InstanceName,
      ledgerApiPort: Port,
      adminParty: LfPartyId,
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      metrics: TrafficEnforcementMetrics,
  )(implicit
      ec: ExecutionContextIdlenessExecutorService,
      tracer: Tracer,
  ): TrafficEnforcementBackendImpl = {
    val trafficServiceClient = trafficEnforcementServerConfig match {
      case internal: TrafficEnforcementServerConfig.Internal =>
        RichTrafficServiceClient.toInternalServer(
          grpcChannelName = internal.processServerNameForInstance(instanceName, ledgerApiPort),
          timeout = processingTimeout,
          accountLookupTimeout = internal.accountLookupTimeout,
          loggerFactory = loggerFactory,
        )
    }

    new TrafficEnforcementBackendImpl(
      enforceCostOnSubmissions,
      rejectMultiPartySubmissions,
      allowSubmissionsOnDegradation,
      trafficServiceClient,
      adminParty,
      metrics,
      processingTimeout,
      loggerFactory,
    )
  }

  implicit class DynamicDecorator(val backend: Eval[TrafficEnforcementBackend]) {
    def dynamic: TrafficEnforcementBackend =
      new TrafficEnforcementBackend {
        override def validateTraffic(actAs: Seq[LfPartyId], trafficCost: Long)(implicit
            traceContext: TraceContext
        ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.TrafficEnforcementError, Unit] =
          backend.value.validateTraffic(actAs, trafficCost)

        override def trafficServiceClient: RichTrafficServiceClient =
          backend.value.trafficServiceClient
      }
  }
}
