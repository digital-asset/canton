// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{BroadcastHub, Keep, Source, SourceQueueWithComplete}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerMemberId
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.AkkaUtil

import scala.concurrent.{ExecutionContext, Future}

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

  private val ((queue, killSwitch), notificationsHubSource) = AkkaUtil.runSupervised(
    logger.error("LocalStateEventSignaller flow failed", _)(TraceContext.empty), {
      Source
        .queue[WriteNotification](1, OverflowStrategy.backpressure)
        .conflate(_ union _)
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(BroadcastHub.sink(1))(Keep.both)
    },
  )

  override def notifyOfLocalWrite(
      notification: WriteNotification
  )(implicit traceContext: TraceContext): Future[Unit] =
    performUnlessClosingF {
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

  private def queueWithLogging[A](name: String, queue: SourceQueueWithComplete[A])(
      item: A
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.debug(s"Pushing item to $name: $item")
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
    Seq(
      SyncCloseable("queue.complete", queue.complete()),
      SyncCloseable("killSwitch.shutdown", killSwitch.shutdown()),
      AsyncCloseable("queue.completion", queue.watchCompletion(), timeouts.shutdownShort.unwrap),
    )
  }
}
