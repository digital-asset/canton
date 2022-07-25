// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import cats.syntax.traverse._
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.domain.sequencing.sequencer.errors._
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{SendAsyncError, SubmissionRequest}
import com.digitalasset.canton.topology.{DomainTopologyManagerId, Member, UnauthenticatedMemberId}
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
)(implicit executionContext: ExecutionContext, trace: Tracer)
    extends Sequencer
    with NamedLogging
    with Spanning {

  private val healthListeners = new AtomicReference[List[SequencerHealthStatus => Unit]](Nil)
  private val currentHealth =
    new AtomicReference[SequencerHealthStatus](SequencerHealthStatus(isActive = true))

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
        //TODO(danilo/arne) consider whether to propagate these errors further
        logger.error(s"An unexpected error occurred whilst registering member $member: $message")
        EitherT.pure[Future, WriteRequestRefused](())
      case error: WriteRequestRefused => EitherT.leftT(error)
    }

  override def sendAsync(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] =
    withSpan("Sequencer.sendAsync") { implicit traceContext => span =>
      span.setAttribute("sender", submission.sender.toString)
      span.setAttribute("message_id", submission.messageId.unwrap)
      for {
        _ <- (for {
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
        _ <- sendAsyncInternal(submission)
      } yield ()
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
      listener: SequencerHealthStatus => Unit
  )(implicit traceContext: TraceContext): Unit = {
    healthListeners.getAndUpdate(listener :: _)
    listener(currentHealth.get())
  }

  protected def healthChanged(health: SequencerHealthStatus): Unit = {
    currentHealth.set(health)
    healthListeners.get().foreach(_(health))
  }

  protected def sendAsyncInternal(submission: SubmissionRequest)(implicit
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
}
