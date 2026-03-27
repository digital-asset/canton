// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.CantonErrorGroups.SequencerSubscriptionErrorGroup
import com.digitalasset.canton.error.{CantonError, ContextualizedCantonError}
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, FlagCloseableAsync, SyncCloseable}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging}
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{Future, Promise}

/** Why did the sequencer subscription terminate */
sealed trait SubscriptionCloseReason[+E]

object SubscriptionCloseReason {

  final case class HandlerError[E](error: E) extends SubscriptionCloseReason[E]

  /** The handler threw an exception */
  final case class HandlerException(exception: Throwable) extends SubscriptionCloseReason[Nothing]

  /** The subscription itself failed. [[com.digitalasset.canton.sequencing.SequencerConnectionX]]
    * implementations are expected to provide their own hierarchy of errors and supply a matching
    * [[SubscriptionErrorRetryPolicy]] to the [[SequencerClient]] for determining which errors are
    * appropriate for attempting to resume a subscription.
    */
  trait SubscriptionError extends SubscriptionCloseReason[Nothing]

  /** The subscription was denied Implementations are expected to provide their own error of this
    * type
    */
  trait PermissionDeniedError extends SubscriptionCloseReason[Nothing]

  /** The sequencer connection details are being updated, so the subscription is being closed so
    * another one is created with the updated transport. This is not an error and also not a reason
    * to close the sequencer client.
    */
  case object TransportChange extends SubscriptionCloseReason[Nothing]

  /** The subscription was closed by the client. */
  case object Closed extends SubscriptionCloseReason[Nothing]

  /** The subscription was closed due to an ongoing shutdown procedure. */
  case object Shutdown extends SubscriptionCloseReason[Nothing]

  /** The subscription was closed by the server due to a token expiration. */
  case object TokenExpiration extends SubscriptionCloseReason[Nothing]
}

/** A running subscription to a sequencer. Can be closed by the consumer or the producer. Once
  * closed the [[closeReason]] value will be fulfilled with the reason the subscription was closed.
  * Implementations are expected to immediately start their subscription unless otherwise stated. If
  * close is called while the handler is running closeReason should not be completed until the
  * handler has completed.
  */
trait InternallyCompletedSequencerSubscription[HandlerError]
    extends FlagCloseableAsync
    with NamedLogging {

  protected val closeReasonPromise: Promise[SubscriptionCloseReason[HandlerError]] =
    Promise[SubscriptionCloseReason[HandlerError]]()

  /** Future which is completed when the subscription is closed. If the subscription is closed in a
    * healthy state the future will be completed successfully. However if the subscription fails for
    * an unexpected reason at runtime the completion should be failed.
    */
  val closeReason: Future[SubscriptionCloseReason[HandlerError]] = closeReasonPromise.future

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    Seq(
      SyncCloseable(
        "sequencer-subscription",
        closeReasonPromise.trySuccess(SubscriptionCloseReason.Closed).discard,
      )
    )

  // We don't want to throw here when closing the subscription fails (e.g in case of timeout)
  // If we threw we could short circuit the rest of the cleaning up of the gRPC stream and end up with
  // a stalled stream
  override def onCloseFailure(e: Throwable): Unit =
    logger.warn("Failed to close sequencer subscription", e)(TraceContext.empty)
}

/** A subscription to a sequencer that can also be completed externally.
  */
trait SequencerSubscription[HandlerError]
    extends InternallyCompletedSequencerSubscription[HandlerError] {

  /** Completes the subscription with the given reason and closes it. */
  private[canton] def complete(reason: SubscriptionCloseReason[HandlerError])(implicit
      traceContext: TraceContext
  ): Unit
}

object SequencerSubscriptionError extends SequencerSubscriptionErrorGroup {

  sealed trait SequencedEventError extends ContextualizedCantonError

  @Explanation(
    """This error indicates that a sequencer subscription to a recently onboarded sequencer attempted to read
      |an event replaced with a tombstone. A tombstone occurs if the timestamp associated with the event predates
      |the validity of the sequencer's signing key. This error results in the sequencer client disconnecting from
      |the sequencer."""
  )
  @Resolution(
    """Connect to another sequencer with older event history to consume the tombstoned events
      |before reconnecting to the recently onboarded sequencer."""
  )
  object TombstoneEncountered
      extends ErrorCode(
        id = "SEQUENCER_TOMBSTONE_ENCOUNTERED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(override val cause: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause)
        with SequencedEventError

    object Error {
      def apply(
          sequencingTimestamp: CantonTimestamp,
          member: Member,
          timestamp: CantonTimestamp,
      )(implicit
          loggingContext: ErrorLoggingContext
      ): Error = new Error(
        s"This sequencer cannot sign the event with sequencing timestamp $sequencingTimestamp for member $member at signing timestamp $timestamp, delivering a tombstone and terminating the subscription."
      )
    }
  }

  @Explanation(
    """This warning is logged when a sequencer subscription is interrupted. The system will keep on retrying to reconnect indefinitely."""
  )
  @Resolution(
    "Monitor the situation and contact the server operator if the issue does not resolve itself automatically."
  )
  object LostSequencerSubscription
      extends ErrorCode(
        "SEQUENCER_SUBSCRIPTION_LOST",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {

    final case class Warn(sequencer: SequencerId, _logOnCreation: Boolean = true)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Lost subscription to sequencer $sequencer. Will try to recover automatically."
        ) {
      override def logOnCreation: Boolean = _logOnCreation
    }
  }

  @Explanation(
    """This error is logged when a sequencer client determined a ledger fork, where a sequencer node
      |responded with different events for the same timestamp / counter.
      |
      |Whenever a client reconnects to a synchronizer, it will start with the last message received and compare
      |whether that last message matches the one it received previously. If not, it will report with this error.
      |
      |A ledger fork should not happen in normal operation. It can happen if the backups have been taken
      |in a wrong order and e.g. the participant was more advanced than the sequencer.
      |"""
  )
  @Resolution(
    """You can recover by restoring the system with a correctly ordered backup. Please consult the
      |respective sections in the manual."""
  )
  object ForkHappened
      extends ErrorCode(
        "SEQUENCER_FORK_DETECTED",
        ErrorCategory.SystemInternalAssumptionViolated,
      )

}

/** Errors that may occur on the creation of a sequencer subscription
  */
sealed trait SequencerSubscriptionCreationError extends SubscriptionCloseReason.SubscriptionError

/** When a fatal error occurs on the creation of a sequencer subscription, the subscription will not
  * retry the subscription creation. Instead, the subscription will fail.
  */
final case class Fatal(msg: String) extends SequencerSubscriptionCreationError
