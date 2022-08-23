// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerMemberId
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{AkkaUtil, LoggerUtil}
import io.functionmeta.functionFullName
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future, TimeoutException}

/** If all Sequencer writes are occurring locally we pipe write notifications to read subscriptions allowing the
  * [[SequencerReader]] to immediately read from the backing store rather than polling.
  *
  * An important caveat is that we only supply signals when a write for a member occurs. If there are no writes from
  * starting the process the member will never receive a read signal. The [[SequencerReader]] is responsible for
  * performing at least one initial read from the store to ensure that all prior events are served as required.
  *
  * Not suitable or at least very sub-optimal for a horizontally scaled sequencer setup where a reader will not have
  * visibility of all writes locally.
  */
class LocalSequencerStateEventSignaller(
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer, executionContext: ExecutionContext)
    extends EventSignaller
    with FlagCloseableAsync
    with NamedLogging {

  private val (((queue, terminateBeforeConflate), terminateAfterConflate), notificationsHubSource) =
    AkkaUtil.runSupervised(
      logger.error("LocalStateEventSignaller flow failed", _)(TraceContext.empty),
      Source
        .queue[WriteNotification](1, OverflowStrategy.backpressure)
        // TODO(#9883) Remove
        .watchTermination()(Keep.both)
        .map(logWriteNotification("before-conflate"))
        .conflate(_ union _)
        // TODO(#9883) Remove
        .watchTermination()(Keep.both)
        .map(logWriteNotification("after-conflate"))
        .toMat(BroadcastHub.sink(1))(Keep.both),
    )

  private def logWriteNotification(
      name: String
  )(notification: WriteNotification): WriteNotification = {
    noTracingLogger.debug(s"$name: Write notification $notification passes through")
    notification
  }

  override def notifyOfLocalWrite(
      notification: WriteNotification
  )(implicit traceContext: TraceContext): Future[Unit] =
    performUnlessClosingF(functionFullName) {
      queueWithLogging("latest-head-state-queue", queue)(notification)
    }.onShutdown {
      logger.info("Dropping local write signal due to shutdown")
      // Readers/subscriptions should be shut down separately
    }

  override def readSignalsForMember(
      member: Member,
      memberId: SequencerMemberId,
  ): Source[ReadSignal, NotUsed] =
    notificationsHubSource
      .filter(_.includes(memberId))
      .map(_ => ReadSignal)

  private def queueWithLogging(name: String, queue: SourceQueueWithComplete[WriteNotification])(
      item: WriteNotification
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val logLevel = item match {
      case WriteNotification.None => Level.TRACE
      case _: WriteNotification.Members => Level.DEBUG
    }
    LoggerUtil.logAtLevel(logLevel, s"Pushing item to $name: $item")
    queue.offer(item) map {
      case result: QueueCompletionResult =>
        logger.warn(s"Failed to queue item on $name: $result")
      case QueueOfferResult.Enqueued =>
      case QueueOfferResult.Dropped =>
        logger.warn(s"Dropped item while trying to queue on $name")
    }
  }

  protected override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty._

    val logAllThreads: Thread => Boolean = _ => true
    def dumpExecutionContextOnTimeout(timeout: TimeoutException): Unit = {
      logger.debug(s"Execution context dump at timeout:\n${executionContext.toString}", timeout)
      logger.debug(s"Akka execution context: ${materializer.executionContext.toString}")
    }

    Seq(
      SyncCloseable("queue.complete", queue.complete()),
      AsyncCloseable(
        "queue.completion", {
          // Create a new subscription from the broadcast hub to make sure that
          // the completion really reaches the hub.
          // Just watching the queue's completion merely synchronizes on when
          // the queue's elements have been passed downstream,
          // not that they have reached the hub.
          val newSubscription = notificationsHubSource.runWith(Sink.ignore)

          // Somewhere on the way, we're losing the termination of the source.
          // Let's monitor it.
          // TODO(#9883) remove
          timeouts.shutdownShort.await(
            "queue.completion.before.conflate",
            stackTraceFilter = logAllThreads,
            onTimeout = dumpExecutionContextOnTimeout,
          )(terminateBeforeConflate)
          logger.debug("queue.completion has terminated before conflate")
          timeouts.shutdownShort.await(
            "queue.completion.after.conflate",
            stackTraceFilter = logAllThreads,
            onTimeout = dumpExecutionContextOnTimeout,
          )(terminateAfterConflate)
          logger.debug("queue.completion has terminated after conflate")
          timeouts.shutdownShort.await(
            "queue.watchCompletion",
            stackTraceFilter = logAllThreads,
            onTimeout = dumpExecutionContextOnTimeout,
          )(queue.watchCompletion())
          newSubscription
        },
        timeouts.shutdownShort.unwrap,
      ),
      // Other readers of the broadcast hub should be shut down separately
    )
  }
}
