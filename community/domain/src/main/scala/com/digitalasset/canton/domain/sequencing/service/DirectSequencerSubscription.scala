// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}
import cats.syntax.either.*
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.{SequencerSubscription, SubscriptionCloseReason}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{AkkaUtil, FutureUtil, SingleUseCell}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Subscription connected directly to a [[sequencer.Sequencer]].
  * Should be created with [[DirectSequencerSubscriptionFactory]].
  */
private[service] class DirectSequencerSubscription[E](
    member: Member,
    source: Sequencer.EventSource,
    handler: SerializedEventHandler[E],
    override protected val timeouts: ProcessingTimeout,
    baseLoggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends SequencerSubscription[E]
    with FlagCloseableAsync
    with NoTracing {

  protected val loggerFactory: NamedLoggerFactory =
    baseLoggerFactory.append("member", show"${member}")

  private val externalCompletionRef: SingleUseCell[SubscriptionCloseReason[E]] =
    new SingleUseCell[SubscriptionCloseReason[E]]()

  private val ((killSwitch, sourceDone), done) = AkkaUtil.runSupervised(
    logger.error("Fatally failed to handle event", _),
    source
      .mapAsync(1) { event =>
        externalCompletionRef.get match {
          case None =>
            performUnlessClosingF("direct-sequencer-subscription-handler") {
              handler(event)
            }.onShutdown {
              Right(())
            }.map(_.leftMap(SubscriptionCloseReason.HandlerError(_)))
          case Some(reason) => Future.successful(Left(reason))
        }
      }
      .collect { case Left(err) => err }
      .take(1)
      .toMat(Sink.headOption)(Keep.both),
  )

  FutureUtil.doNotAwait(
    done.thereafter {
      case Success(None) =>
        logger.debug(show"Subscription flow for $member has completed")
        closeReasonPromise.trySuccess(SubscriptionCloseReason.Closed).discard[Boolean]
      case Success(Some(SubscriptionCloseReason.TransportChange)) =>
        logger.debug(show"Subscription flow for $member has completed due to transport change")
        closeReasonPromise.trySuccess(SubscriptionCloseReason.TransportChange).discard[Boolean]
      case Success(Some(error)) =>
        logger.warn(s"Subscription handler returned error: $error")
        closeReasonPromise.trySuccess(error).discard[Boolean]
      case Failure(ex) =>
        logger.warn(show"Subscription flow for $member has failed", ex)
        closeReasonPromise.tryFailure(ex).discard[Boolean]
    },
    s"DirectSequencerSubscription for $member failed",
  )

  override private[canton] def complete(reason: SubscriptionCloseReason[E])(implicit
      traceContext: TraceContext
  ): Unit = {
    externalCompletionRef.putIfAbsent(reason).discard
    close()
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
    SyncCloseable(s"killing direct-sequencer-subscription for $member", killSwitch.shutdown()),
    AsyncCloseable(
      s"flushing direct-sequencer-subscription for $member",
      done,
      timeouts.shutdownNetwork.duration,
    ),
    AsyncCloseable(
      s"flushing other sinks in direct-sequencer-subscription for $member",
      sourceDone,
      timeouts.shutdownNetwork.duration,
    ),
  )
}
