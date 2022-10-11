// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.traverse.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose}
import com.digitalasset.canton.domain.sequencing.sequencer.errors.*
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{
  SendAsyncError,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.time.{Clock, PeriodicAction}
import com.digitalasset.canton.topology.{DomainTopologyManagerId, Member, UnauthenticatedMemberId}
import com.digitalasset.canton.tracing.Spanning.SpanWrapper
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.EitherTUtil.ifThenET
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Provides additional functionality that is common between sequencer implementations:
  *  - auto registers unknown recipients addressed in envelopes from the domain topology manager
  *    (avoids explicit registration from the domain node -> sequencer which will be useful when separate processes)
  */
abstract class BaseSequencer(
    domainManagerId: DomainTopologyManagerId,
    protected val loggerFactory: NamedLoggerFactory,
    healthConfig: Option[SequencerHealthConfig],
    clock: Clock,
    checkSignature: TraceContext => SignedContent[SubmissionRequest] => EitherT[
      Future,
      SendAsyncError,
      SignedContent[SubmissionRequest],
    ],
)(implicit executionContext: ExecutionContext, trace: Tracer)
    extends Sequencer
    with NamedLogging
    with Spanning {

  private val healthListeners =
    new AtomicReference[List[(SequencerHealthStatus, TraceContext) => Unit]](Nil)
  private val currentHealth =
    new AtomicReference[SequencerHealthStatus](SequencerHealthStatus(isActive = true))

  val periodicHealthCheck: Option[PeriodicAction] = healthConfig.map(conf =>
    // periodically calling the sequencer's health check in order to continuously notify
    // listeners in case the health status has changed.
    new PeriodicAction(
      clock,
      conf.backendCheckPeriod,
      loggerFactory,
      timeouts,
      "health-check",
    )(tc => health(tc))
  )

  /** The domain manager is responsible for identities within the domain.
    * If they decide to address a message to a member then we can be confident that they want the member available on the sequencer.
    * No other member gets such privileges.
    */
  private def autoRegisterNewMembersMentionedByIdentityManager(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, WriteRequestRefused, Unit] = {
    def ensureAllRecipientsRegistered: EitherT[Future, WriteRequestRefused, Unit] =
      submission.batch.allRecipients.toList
        .traverse(ensureMemberRegistered)
        .map(_ => ())

    for {
      _ <- ifThenET(submission.sender == domainManagerId)(ensureAllRecipientsRegistered)
    } yield ()
  }

  private def ensureMemberRegistered(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, WriteRequestRefused, Unit] =
    ensureRegistered(member).leftFlatMap {
      // due to the way ensureRegistered executes it is unlikely (I think impossible) for the already registered error
      // to be returned, however in this circumstance it's actually fine as we want them registered regardless.
      case OperationError(RegisterMemberError.AlreadyRegisteredError(member)) =>
        logger.debug(
          s"Went to auto register member but found they were already registered: $member"
        )
        EitherT.pure[Future, WriteRequestRefused](())
      case OperationError(RegisterMemberError.UnexpectedError(member, message)) =>
        // TODO(danilo/arne) consider whether to propagate these errors further
        logger.error(s"An unexpected error occurred whilst registering member $member: $message")
        EitherT.pure[Future, WriteRequestRefused](())
      case error: WriteRequestRefused => EitherT.leftT(error)
    }

  override def sendAsyncSigned(signedSubmission: SignedContent[SubmissionRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit] = withSpan("Sequencer.sendAsyncSigned") {
    implicit traceContext => span =>
      val submission = signedSubmission.content
      span.setAttribute("sender", submission.sender.toString)
      span.setAttribute("message_id", submission.messageId.unwrap)
      for {
        _ <- checkMemberRegistration(submission)
        signedSubmissionWithFixedTs <- checkSignature(traceContext)(signedSubmission)
        _ <- sendAsyncSignedInternal(signedSubmissionWithFixedTs)
      } yield ()
  }

  override def sendAsync(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] =
    withSpan("Sequencer.sendAsync") { implicit traceContext => span =>
      setSpanAttributes(span, submission)
      for {
        _ <- checkMemberRegistration(submission)
        _ <- sendAsyncInternal(submission)
      } yield ()
    }

  private def setSpanAttributes(span: SpanWrapper, submission: SubmissionRequest): Unit = {
    span.setAttribute("sender", submission.sender.toString)
    span.setAttribute("message_id", submission.messageId.unwrap)
  }

  private def checkMemberRegistration(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] = (for {
    _ <- autoRegisterNewMembersMentionedByIdentityManager(submission)
    _ <- submission.sender match {
      case member: UnauthenticatedMemberId =>
        ensureMemberRegistered(member)
      case _ => EitherT.pure[Future, WriteRequestRefused](())
    }
  } yield ()).leftSemiflatMap { registrationError =>
    logger.error(s"Failed to auto-register members: $registrationError")
    // this error won't exist once sendAsync is fully implemented, so temporarily we'll just return a failed future
    Future.failed(
      new RuntimeException(s"Failed to auto-register members: $registrationError")
    )
  }

  override def health(implicit traceContext: TraceContext): Future[SequencerHealthStatus] = for {
    newHealth <- healthInternal
  } yield {
    val old = currentHealth.getAndSet(newHealth)
    if (old != newHealth) healthChanged(newHealth)
    newHealth
  }

  protected def healthInternal(implicit traceContext: TraceContext): Future[SequencerHealthStatus]

  override def onHealthChange(
      listener: (SequencerHealthStatus, TraceContext) => Unit
  )(implicit traceContext: TraceContext): Unit = {
    healthListeners.getAndUpdate(listener :: _)
    listener(currentHealth.get(), traceContext)
  }

  protected def healthChanged(
      health: SequencerHealthStatus
  )(implicit traceContext: TraceContext): Unit = {
    currentHealth.set(health)
    healthListeners.get().foreach(_(health, traceContext))
  }

  protected def sendAsyncInternal(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit]

  protected def sendAsyncSignedInternal(signedSubmission: SignedContent[SubmissionRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit]

  override def read(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource] =
    for {
      _ <- member match {
        case _: UnauthenticatedMemberId =>
          ensureMemberRegistered(member)
            .leftMap(CreateSubscriptionError.RegisterUnauthenticatedMemberError)
        case _ =>
          EitherT.pure[Future, CreateSubscriptionError](())
      }
      source <- readInternal(member, offset)
    } yield source

  protected def readInternal(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource]

  override def onClosed(): Unit =
    periodicHealthCheck.foreach(Lifecycle.close(_)(logger))

}

object BaseSequencer {
  def checkSignature(
      cryptoApi: DomainSyncCryptoClient
  )(implicit
      executionContext: ExecutionContext
  ): TraceContext => SignedContent[SubmissionRequest] => EitherT[
    Future,
    SendAsyncError,
    SignedContent[SubmissionRequest],
  ] =
    traceContext =>
      signedSubmission => {
        val snapshot = cryptoApi.headSnapshot(traceContext)
        val timestamp = snapshot.ipsSnapshot.timestamp
        signedSubmission
          .verifySignature(
            snapshot,
            signedSubmission.content.sender,
            HashPurpose.SubmissionRequestSignature,
          )
          .leftMap(error => {
            SendAsyncError.RequestRefused(
              s"Sequencer could not verify client's signature ${signedSubmission.timestampOfSigningKey
                  .fold("")(ts => s"at $ts ")}on submission request with sequencer's head snapshot at $timestamp. Error: $error"
            ): SendAsyncError
          })
          // set timestamp to the one used by the receiving sequencer's head snapshot timestamp
          .map(_ => signedSubmission.copy(timestampOfSigningKey = Some(timestamp)))
      }

}
