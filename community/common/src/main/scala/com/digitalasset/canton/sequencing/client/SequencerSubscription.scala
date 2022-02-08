// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.lifecycle.{AsyncCloseable, AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.NamedLogging

import scala.concurrent.{Future, Promise}

/** Why did the sequencer subscription terminate */
sealed trait SubscriptionCloseReason[+E]

object SubscriptionCloseReason {

  case class HandlerError[E](error: E) extends SubscriptionCloseReason[E]

  /** The handler threw an exception */
  case class HandlerException(exception: Throwable) extends SubscriptionCloseReason[Nothing]

  /** The subscription itself failed.
    * [[transports.SequencerClientTransport]] implementations are expected to provide their own hierarchy of errors
    * and supply a matching [[SubscriptionErrorRetryPolicy]] to the [[SequencerClient]] for determining which
    * errors are appropriate for attempting to resume a subscription.
    */
  trait SubscriptionError extends SubscriptionCloseReason[Nothing]

  /** The subscription was denied
    * Implementations are expected to provide their own error of this type
    */
  trait PermissionDeniedError extends SubscriptionCloseReason[Nothing]

  /** The subscription was closed by the client. */
  case object Closed extends SubscriptionCloseReason[Nothing]
}

/** A running subscription to a sequencer.
  * Can be closed by the consumer or the producer.
  * Once closed the [[closeReason]] value will be fulfilled with the reason the subscription was closed.
  * Implementations are expected to immediately start their subscription unless otherwise stated.
  * If close is called while the handler is running closeReason should not be completed until the handler has completed.
  */
trait SequencerSubscription[HandlerError] extends FlagCloseableAsync with NamedLogging {

  protected val closeReasonPromise: Promise[SubscriptionCloseReason[HandlerError]] =
    Promise[SubscriptionCloseReason[HandlerError]]()

  /** Future which is completed when the subscription is closed.
    * If the subscription is closed in a healthy state the future will be completed successfully.
    * However if the subscription fails for an unexpected reason at runtime the completion should be failed.
    */
  val closeReason: Future[SubscriptionCloseReason[HandlerError]] = closeReasonPromise.future

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty._

    Seq(
      AsyncCloseable(
        "sequencer-subscription",
        closeReasonPromise.success(SubscriptionCloseReason.Closed).future,
        timeouts.shutdownNetwork.duration,
      )
    )
  }
}
