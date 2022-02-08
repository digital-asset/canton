// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitch, Materializer}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{AsyncCloseable, AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing._
import com.digitalasset.canton.sequencing.client.{SequencerSubscription, SubscriptionCloseReason}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.AkkaUtil
import com.digitalasset.canton.util.ShowUtil._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Subscription connected directly to a [[sequencer.Sequencer]].
  * Should be created with [[DirectSequencerSubscriptionFactory]].
  */
private[service] class DirectSequencerSubscription[E](
    member: Member,
    source: Source[OrdinarySerializedEvent, KillSwitch],
    handler: SerializedEventHandler[E],
    override protected val timeouts: ProcessingTimeout,
    baseLoggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends SequencerSubscription[E]
    with FlagCloseableAsync
    with NoTracing {

  protected val loggerFactory: NamedLoggerFactory =
    baseLoggerFactory.append("member", show"${member}")

  val (killSwitch, done) = AkkaUtil.runSupervised(
    logger.error("Fatally failed to handle event", _),
    source
      .mapAsync(1) { event =>
        performUnlessClosingF {
          handler(event)
        }.onShutdown {
          Right(())
        }
      }
      .collect { case Left(err) => err }
      .take(1)
      .toMat(Sink.headOption)(Keep.both),
  )

  done onComplete {
    case Success(Some(error)) =>
      logger.warn(s"Subscription handler returned error: $error")
      closeReasonPromise.trySuccess(SubscriptionCloseReason.HandlerError(error))
    case Success(_) =>
      logger.debug(show"Subscription flow for $member has completed")
      closeReasonPromise.trySuccess(SubscriptionCloseReason.Closed)
    case Failure(ex) =>
      logger.warn(show"Subscription flow for $member has failed", ex)
      closeReasonPromise.tryFailure(ex)
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
    AsyncCloseable(
      s"direct-sequencer-subscription for $member", {
        killSwitch.shutdown()
        done
      },
      timeouts.shutdownNetwork.duration,
    )
  )
}
