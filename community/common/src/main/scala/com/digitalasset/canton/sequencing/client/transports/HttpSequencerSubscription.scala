// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, Materializer}
import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{AsyncCloseable, AsyncOrSyncCloseable, SyncCloseable}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.client.http.{
  HttpSequencerClient,
  HttpSequencerClientError,
}
import com.digitalasset.canton.sequencing.client.{SequencerSubscription, SubscriptionCloseReason}
import com.digitalasset.canton.sequencing.protocol.SubscriptionRequest
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, SerializedEventHandler}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, Traced}
import com.digitalasset.canton.util.Thereafter.syntax._
import com.digitalasset.canton.util.{AkkaUtil, FutureUtil, SingleUseCell}
import com.digitalasset.canton.{DiscardOps, SequencerCounter}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Subscription to a sequencer over HTTP.
  * Works by polling `readNextEvent` - if an event is found it will immediately try to read the next, otherwise it will delay for `pollingInterval`.
  */
class HttpSequencerSubscription[E] private[transports] (
    startingFromNextCounter: SequencerCounter,
    handler: SerializedEventHandler[E],
    readNextEvent: Traced[SequencerCounter] => EitherT[Future, HttpSequencerClientError, Option[
      OrdinarySerializedEvent
    ]],
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    pollingInterval: FiniteDuration = 50.millis,
)(implicit materializer: Materializer, executionContext: ExecutionContext)
    extends SequencerSubscription[E]
    with NoTracing {

  private val externalCompletionRef: SingleUseCell[SubscriptionCloseReason[E]] =
    new SingleUseCell[SubscriptionCloseReason[E]]()

  private val (killSwitch, done) = AkkaUtil.runSupervised(
    logger.error("Http Sequencer Unexpected Exception", _),
    readEvents(startingFromNextCounter)
      .viaMat(KillSwitches.single)(Keep.right)
      .mapAsync[Either[SubscriptionCloseReason[E], Unit]](1) {
        case Right(event) =>
          // If the subscription was completed externally with a reason,
          // stop handling events and propagate the reason
          externalCompletionRef.get match {
            case None =>
              // call handler ensuring that exceptions thrown from calls are lifted into failed Futures
              val handlerResultEF = Try(handler(event))
                .fold(Future.failed, identity)

              handlerResultEF transform {
                case Failure(exception) =>
                  Success(Left(SubscriptionCloseReason.HandlerException(exception)))
                case Success(Left(error)) =>
                  Success(Left(SubscriptionCloseReason.HandlerError(error)))
                case Success(Right(_)) => Success(Right(()))
              }
            case Some(reason) =>
              Future.successful(Left(reason))
          }
        case Left(error) =>
          Future.successful(Left(SubscriptionReadError(error)))
      }
      .collect {
        // collect just the first error
        // with the below headOption this will cause the stream to complete on the first error
        case Left(closeReason) => closeReason
      }
      .toMat(Sink.headOption)(Keep.both),
  )

  // pass on why the subscription has been completed/closed
  FutureUtil.doNotAwait(
    done.thereafter {
      case Success(closeReason) =>
        // the stream will complete on the first error if found
        // if there is no reason it means the stream was closed by the client
        closeReasonPromise
          .trySuccess(closeReason getOrElse SubscriptionCloseReason.Closed)
          .discard[Boolean]
      case Failure(ex) =>
        closeReasonPromise.tryFailure(ex).discard[Boolean]
    },
    s"http sequencer subscription from $startingFromNextCounter failed",
  )

  override private[canton] def complete(reason: SubscriptionCloseReason[E])(implicit
      traceContext: TraceContext
  ): Unit = {
    externalCompletionRef.putIfAbsent(reason).discard
    close()
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    Seq(
      SyncCloseable("killSwitch.shutdown", killSwitch.shutdown()),
      AsyncCloseable(
        "http-sequencer-subscription",
        done,
        timeouts.shutdownNetwork.duration,
      ),
    )
  }

  /** Poll the backend for each counter until the event is available.
    * This works by starting a stream from the requested counter and then for each item
    * creating a sub-stream that will repeatedly attempt to fetch the event until it is
    * found (the collect and take(1)). If the backend returns an empty result we'll add
    * an initialDelay to pause for a short interval before attempting to check again.
    */
  @VisibleForTesting
  private[transports] def readEvents(
      nextCounter: SequencerCounter
  ): Source[Either[HttpSequencerClientError, OrdinarySerializedEvent], NotUsed] =
    Source
      // read all event sequentially from the provided counter
      .fromIterator(() => Iterator.iterate(nextCounter)(_ + 1))
      // for each counter create a source that we will concat items into the parent source
      // this source must contain only a single item with the event
      .flatMapConcat { counter =>
        // create a source to repeatedly attempt reading an event for this counter
        // if it isn't yet available an empty value will be returned
        // Source.repeat(counter) creates never ending stream of this counter value to attempt reads
        // .take(1) will finally complete the stream once an event is found
        // we introduce a polling delay of `pollingInterval` between attempts if no event is found
        Source
          .repeat(counter)
          .throttle(1, pollingInterval)
          .flatMapConcat(counter => Source.future(readNextEvent(Traced.empty(counter)).value))
          .collect {
            // only publish errors or events we've found
            case Left(err) => Left(err)
            case Right(Some(event)) => Right(event)
          }
          .take(1)
      }

}

object HttpSequencerSubscription {

  def apply[E](
      request: SubscriptionRequest,
      handler: SerializedEventHandler[E],
      client: HttpSequencerClient,
      timeouts: ProcessingTimeout,
      requiresAuthentication: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): HttpSequencerSubscription[E] = {
    new HttpSequencerSubscription(
      request.counter,
      handler,
      client.readNextEvent(request.member, requiresAuthentication),
      timeouts,
      loggerFactory,
    )
  }

}
