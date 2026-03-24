// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberId
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{BroadcastHub, Keep, Sink, Source}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** If all Sequencer writes are occurring locally we pipe write notifications to read subscriptions
  * allowing the [[SequencerReader]] to immediately read from the backing store rather than polling.
  *
  * An important caveat is that we only supply signals when a write for a member occurs. If there
  * are no writes from starting the process the member will never receive a read signal. The
  * [[SequencerReader]] is responsible for performing at least one initial read from the store to
  * ensure that all prior events are served as required.
  *
  * Not suitable or at least very sub-optimal for a horizontally scaled sequencer setup where a
  * reader will not have visibility of all writes locally.
  */
class LocalSequencerStateEventBroadcast(
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer, executionContext: ExecutionContext)
    extends EventSignaller
    with FlagCloseableAsync
    with NamedLogging {

  override def isLegacySignaller: Boolean = true

  private val (queue, notificationsHubSource) = {
    import TraceContext.Implicits.Empty.*
    PekkoUtil.runSupervised(
      Source
        .queue[WriteNotification](1, OverflowStrategy.backpressure)
        // this conflate kicks in, when there is no downstream consumer, to not exert backpressure to the upstream producer
        .conflate(_ union _) // keep the trace context of the latest notification
        .async
        .toMat(
          BroadcastHub
            .sink(1)
            // the default input buffer is 16, but due to the conflation, we don't actually
            // want to buffer many stream elements here
            .addAttributes(Attributes.inputBuffer(1, 1))
        )(Keep.both),
      errorLogMessagePrefix = "LocalStateEventSignaller flow failed",
    )
  }

  override def notifyOfLocalWrite(
      notification: WriteNotification
  ): Future[Unit] = {
    import TraceContext.Implicits.Empty.emptyTraceContext
    synchronizeWithClosingF(functionFullName) {
      queue
        .offer(notification)
        .transform {
          case Success(result: QueueCompletionResult) =>
            logger.warn(s"Failed to queue item: $result")
            Success(())
          case Success(QueueOfferResult.Enqueued) =>
            logger.trace("Push successful.")
            Success(())
          case Success(QueueOfferResult.Dropped) =>
            logger.warn(s"Dropped item while trying to queue")
            Success(())
          case Failure(ex) =>
            logger.warn(s"Pushing item failed with an exception", ex)
            Failure(ex)
        }

    }.onShutdown {
      logger.info("Dropping local write signal due to shutdown")
      // Readers/subscriptions should be shut down separately
    }
  }

  override def readSignalsForMember(
      member: Member,
      memberId: SequencerMemberId,
  )(implicit traceContext: TraceContext): Source[ReadSignal, NotUsed] =
    notificationsHubSource
      .filter(_.isBroadcastOrIncludes(memberId))
      .map(_ => ReadSignal)
      // this conflate ensures that a slow consumer doesn't cause backpressure and therefore
      // block the stream of signals for other consumers
      .conflate((_, right) => right)

  protected override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*

    Seq(
      SyncCloseable("queue.complete", queue.complete()),
      AsyncCloseable(
        "queue.watchCompletion",
        queue.watchCompletion(),
        timeouts.shutdownShort,
      ),
      // `watchCompletion` completes when the queue's contents have been consumed by the `conflate`,
      // but `conflate` need not yet have passed the conflated element to the BroadcastHub.
      // So we create a new subscription and wait until the completion signal has propagated.
      AsyncCloseable(
        "queue.completion",
        notificationsHubSource.runWith(Sink.ignore),
        timeouts.shutdownShort,
      ),
      // Other readers of the broadcast hub should be shut down separately
    )
  }
}
