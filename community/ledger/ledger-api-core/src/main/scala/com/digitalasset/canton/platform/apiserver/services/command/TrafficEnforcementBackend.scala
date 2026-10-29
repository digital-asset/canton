// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.*
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.v1.RegisterSubmissionRequest.ChargedAccount
import com.digitalasset.canton.tea.v1.{GetAccountRequest, RegisterSubmissionRequest}
import com.digitalasset.canton.tracing.Spanning.SpanWrapper
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.data.Ref
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext

trait TrafficEnforcementBackend {

  /** Stores the account to charge and checks if its balance covers `trafficCost` when enforced.
    * Called just before sending the confirmation request. TEA errors return as `Left`, not failed
    * futures.
    */
  def registerSubmission(
      info: SubmissionAccountingInfo,
      trafficCost: Long,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit]

  /** Checks if the selected account's balance covers the estimated cost, without storing anything.
    * Used when preparing an interactive submission.
    */
  def checkEstimatedCost(
      accounting: SubmissionAccounting,
      estimatedCost: Long,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.TrafficEnforcementError, Unit]

  def adminParty: LfPartyId

  def trafficServiceClient: RichTrafficServiceClient
}

/** Service used for enforcing user-level traffic limits on the participant node, as part of
  * submission requests in the Phase 1 of the Canton protocol.
  *
  * @param enforceCostOnSubmissions
  *   Whether to check the account balance when registering a submission. When disabled, the
  *   submission is still registered, just without a balance check.
  * @param rejectMultiPartySubmissions
  *   Whether to reject submissions without an account whose actAs has more than one party (or
  *   none), instead of registering them with a no-account marker.
  * @param allowSubmissionsOnDegradation
  *   Whether to let submissions proceed if account lookup or registration fails without a response
  *   from the traffic service. Correlation conflicts and refusals still fail the submission.
  *   No-account marker writes are always best effort.
  * @param trafficServiceClient
  *   The traffic service client used to communicate with the traffic enforcement server.
  */
class TrafficEnforcementBackendImpl(
    enforceCostOnSubmissions: Boolean,
    rejectMultiPartySubmissions: Boolean,
    allowSubmissionsOnDegradation: Boolean,
    override val trafficServiceClient: RichTrafficServiceClient,
    override val adminParty: LfPartyId,
    metrics: TrafficEnforcementMetrics,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends TrafficEnforcementBackend
    with NamedLogging
    with FlagCloseable
    with Spanning {

  override def registerSubmission(
      info: SubmissionAccountingInfo,
      trafficCost: Long,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] = {
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)
    metrics.enforcementCheckDuration.timeEitherFUSWithLabels(
      withSpan(TrafficEnforcementBackend.RegisterSubmissionSpanName) { spanTraceContext => span =>
        register(info, trafficCost, span)(spanTraceContext, errorLoggingContext)
      },
      labelMapping = {
        // Redacted statuses decode without an error id, so we label all internal failures the same.
        case Left(e) if e.code.category.redactDetails => TrafficEnforcementOutcome.Failed
        case Left(e) => e.code.id
        case Right(_) => "success"
      },
      failedStatus = TrafficEnforcementOutcome.Failed,
    )
  }

  private def register(
      info: SubmissionAccountingInfo,
      trafficCost: Long,
      span: SpanWrapper,
  )(implicit
      traceContext: TraceContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] = {
    logger.debug(
      s"Registering submission ${info.submissionId} with ${info.accounting.selection}, trafficCost: $trafficCost"
    )
    def send(
        accountId: Option[String],
        requiredBalance: Option[Long],
        outcome: String,
        reason: Option[String],
    ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
      sendRegistration(
        RegisterSubmissionRequest(
          changeIdHash = info.changeId.hash.toHexString,
          submissionId = info.submissionId,
          account = accountId.map(ChargedAccount(_, requiredBalance)),
        ),
        span,
        outcome,
        reason,
      )

    def forAccount(accountId: String): EitherT[FutureUnlessShutdown, RpcError, Unit] =
      if (enforceCostOnSubmissions)
        send(Some(accountId), Some(trafficCost), TrafficEnforcementOutcome.Accepted, None)
      else
        send(
          Some(accountId),
          None,
          TrafficEnforcementOutcome.Skipped,
          Some(TrafficEnforcementOutcome.EnforcementDisabled),
        )

    info.accounting.selection match {
      case TrafficAccountSelection.Explicit(accountId) => forAccount(accountId.unwrap)
      case TrafficAccountSelection.FallbackParty(party) => forAccount(party)
      case TrafficAccountSelection.AdminFallback(admin) =>
        logger.debug(
          show"Skipping traffic enforcement validation for participant admin party: $admin"
        )
        send(
          Some(admin),
          None,
          TrafficEnforcementOutcome.Skipped,
          Some(TrafficEnforcementOutcome.AdminParty),
        )
      case TrafficAccountSelection.NoAccount if rejectMultiPartySubmissions =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          rejectNoAccount(
            span,
            show"Traffic enforcement rejected submission with non-singleton actAs parties: ${info.changeId.actAs.toSeq}",
          ): RpcError
        )
      case TrafficAccountSelection.NoAccount =>
        logger.info(
          show"Skipping traffic enforcement validation due to non-singleton actAs parties: ${info.changeId.actAs.toSeq}"
        )
        send(
          None,
          None,
          TrafficEnforcementOutcome.Skipped,
          Some(TrafficEnforcementOutcome.NonSingletonActAs),
        )
    }
  }

  private def sendRegistration(
      request: RegisterSubmissionRequest,
      span: SpanWrapper,
      successOutcome: String,
      successReason: Option[String],
  )(implicit
      traceContext: TraceContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    EitherT(trafficServiceClient.registerSubmission(request).value.map {
      case Right(_) =>
        recordOutcome(span, successOutcome, successReason)
        Right(())
      case Left(grpcError) => registrationFailure(request, grpcError, span)
    })

  private def registrationFailure(
      request: RegisterSubmissionRequest,
      grpcError: GrpcError,
      span: SpanWrapper,
  )(implicit
      traceContext: TraceContext,
      errorLoggingContext: ErrorLoggingContext,
  ): Either[RpcError, Unit] = {
    val teaErrorId = grpcError.decodedCantonError.map(_.code.id)

    def reject(outcome: String, reason: String): Either[RpcError, Unit] = {
      recordOutcome(span, outcome, Some(reason))
      Left(RichTrafficServiceClient.normalizeTeaErrorToRpcError(grpcError))
    }

    request.account.map(_.accountId) match {
      case _ if teaErrorId.contains(TrafficEnforcementErrors.CorrelationConflict.id) =>
        reject(TrafficEnforcementOutcome.Rejected, TrafficEnforcementOutcome.CorrelationConflict)
      case None =>
        // If the marker is lost, the completion only logs a missing-correlation warning.
        degrade(
          span,
          grpcError,
          TrafficEnforcementOutcome.MarkerWriteFailed,
          s"Failed to store the no-account marker for submission ${request.submissionId};" +
            " allowing the submission to proceed without it.",
        )
        Right(())
      case Some(_) if teaErrorId.contains(TrafficEnforcementErrors.InsufficientBalance.id) =>
        reject(TrafficEnforcementOutcome.Rejected, TrafficEnforcementOutcome.InsufficientBalance)
      case Some(accountId)
          if TrafficEnforcementBackend.allowsSubmissionOnLookupFailure(
            allowSubmissionsOnDegradation,
            grpcError,
          ) =>
        degrade(
          span,
          grpcError,
          TrafficEnforcementOutcome.LookupUnavailable,
          s"Traffic enforcement submission registration failed for account $accountId;" +
            " degrading and allowing the submission to proceed without a balance check.",
        )
        Right(())
      case Some(_) =>
        span.recordException(grpcError.asRuntimeException)
        reject(TrafficEnforcementOutcome.Failed, TrafficEnforcementOutcome.LookupFailed)
    }
  }

  private def degrade(
      span: SpanWrapper,
      grpcError: GrpcError,
      reason: String,
      message: String,
  )(implicit traceContext: TraceContext): Unit = {
    logger.warn(s"$message\n$grpcError")
    recordOutcome(span, TrafficEnforcementOutcome.Degraded, Some(reason))
    span.recordException(grpcError.asRuntimeException)
  }

  override def checkEstimatedCost(
      accounting: SubmissionAccounting,
      estimatedCost: Long,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.TrafficEnforcementError, Unit] = {
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)
    metrics.enforcementCheckDuration.timeEitherFUSWithLabels(
      withSpan(TrafficEnforcementBackend.CheckEstimatedCostSpanName) { spanTraceContext => span =>
        decide(accounting, estimatedCost, span)(spanTraceContext, errorLoggingContext)
      },
      labelMapping = {
        case Left(e) => e.code.id
        case Right(_) => "success"
      },
      failedStatus = TrafficEnforcementOutcome.Failed,
    )
  }

  private def decide(
      accounting: SubmissionAccounting,
      trafficCost: Long,
      span: SpanWrapper,
  )(implicit
      traceContext: TraceContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.TrafficEnforcementError, Unit] = {
    logger.debug(
      s"Validating traffic enforcement for ${accounting.selection}, trafficCost: $trafficCost"
    )
    accounting.selection match {
      case TrafficAccountSelection.AdminFallback(admin) =>
        logger.debug(
          show"Skipping traffic enforcement validation for participant admin party: $admin"
        )
        recordOutcome(
          span,
          TrafficEnforcementOutcome.Skipped,
          Some(TrafficEnforcementOutcome.AdminParty),
        )
        EitherT.pure(())
      case TrafficAccountSelection.Explicit(accountId) =>
        validateBalance(accountId.unwrap, trafficCost, span)
      case TrafficAccountSelection.FallbackParty(party) =>
        validateBalance(party, trafficCost, span)
      case TrafficAccountSelection.NoAccount if rejectMultiPartySubmissions =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          rejectNoAccount(
            span,
            "Traffic enforcement rejected submission with non-singleton actAs parties",
          ): TrafficEnforcementErrors.TrafficEnforcementError
        )
      case TrafficAccountSelection.NoAccount =>
        logger.info(
          "Skipping traffic enforcement validation due to non-singleton actAs parties"
        )
        recordOutcome(
          span,
          TrafficEnforcementOutcome.Skipped,
          Some(TrafficEnforcementOutcome.NonSingletonActAs),
        )
        EitherT.pure(())
    }
  }

  private def rejectNoAccount(span: SpanWrapper, cause: String)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): TrafficEnforcementErrors.MultiPartySubmissionRejected.Reject = {
    recordOutcome(
      span,
      TrafficEnforcementOutcome.Rejected,
      Some(TrafficEnforcementOutcome.MultiPartySubmission),
    )
    TrafficEnforcementErrors.MultiPartySubmissionRejected.Reject(cause)
  }

  private def validateBalance(
      accountId: String,
      trafficCost: Long,
      span: SpanWrapper,
  )(implicit
      traceContext: TraceContext,
      errorLoggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementErrors.TrafficEnforcementError, Unit] =
    if (!enforceCostOnSubmissions) {
      recordOutcome(
        span,
        TrafficEnforcementOutcome.Skipped,
        Some(TrafficEnforcementOutcome.EnforcementDisabled),
      )
      EitherT.pure(())
    } else
      EitherT(
        trafficServiceClient
          .getAccount(GetAccountRequest(accountId))
          .value
          .flatMap[Either[TrafficEnforcementErrors.TrafficEnforcementError, Unit]] {
            case Right(account) if account.balance >= trafficCost =>
              recordOutcome(span, TrafficEnforcementOutcome.Accepted)
              FutureUnlessShutdown.pure(Right(()))
            case Right(account) =>
              recordOutcome(
                span,
                TrafficEnforcementOutcome.Rejected,
                Some(TrafficEnforcementOutcome.InsufficientBalance),
              )
              FutureUnlessShutdown.pure(
                Left(
                  TrafficEnforcementErrors.InsufficientBalance.Reject(
                    accountId = accountId,
                    balance = account.balance,
                    trafficCost = trafficCost,
                  )
                )
              )
            case Left(grpcError)
                if TrafficEnforcementBackend.allowsSubmissionOnLookupFailure(
                  allowSubmissionsOnDegradation,
                  grpcError,
                ) =>
              degrade(
                span,
                grpcError,
                TrafficEnforcementOutcome.LookupUnavailable,
                s"Traffic enforcement account lookup failed for account $accountId; degrading" +
                  " and allowing the submission to proceed without a balance check.",
              )
              FutureUnlessShutdown.pure(Right(()))
            case Left(grpcError) =>
              recordOutcome(
                span,
                TrafficEnforcementOutcome.Failed,
                Some(TrafficEnforcementOutcome.LookupFailed),
              )
              // The span picks up the exception and the ERROR status from closeSpan as it propagates.
              FutureUnlessShutdown.failed(RichTrafficServiceClient.normalizeTeaError(grpcError))
          }
      )

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

  private[command] val RegisterSubmissionSpanName: String =
    "TrafficEnforcementBackend.registerSubmission"
  private[command] val CheckEstimatedCostSpanName: String =
    "TrafficEnforcementBackend.checkEstimatedCost"

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

  def registerIfEnabled(
      backendO: Option[TrafficEnforcementBackend],
      changeId: ChangeId,
      submissionId: Option[Ref.SubmissionId],
      accounting: SubmissionAccounting,
      trafficCost: Long,
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, RpcError, Unit] =
    backendO.fold(EitherT.rightT[FutureUnlessShutdown, RpcError](())) { backend =>
      for {
        info <- EitherT.fromEither[FutureUnlessShutdown](
          SubmissionAccountingInfo.create(changeId, submissionId, accounting).leftWiden[RpcError]
        )
        _ <- backend.registerSubmission(info, trafficCost)(errorLoggingContext.traceContext)
      } yield ()
    }

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
}
