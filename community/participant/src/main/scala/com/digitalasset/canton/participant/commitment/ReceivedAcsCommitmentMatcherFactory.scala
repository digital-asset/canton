// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.TickSignaller
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
import com.digitalasset.canton.participant.store.{AcsCommitmentPeriodStore, AcsDigestStore}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, PekkoUtil}
import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, Materializer}

import scala.concurrent.{ExecutionContext, Future}

trait ReceivedAcsCommitmentMatcherFactory {
  def startMatcherPipeline(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      tickSignaller: TickSignaller,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[(KillSwitch, Future[Done])]
}

class ReceivedAcsCommitmentMatcherFactoryImpl(
    periodStoreLookup: SynchronizerId => Option[AcsCommitmentPeriodStore],
    digestStoreLookup: SynchronizerId => Option[AcsDigestStore],
    internalIndexService: InternalIndexService,
    stringInterning: StringInterning,
    parallelProcessingLimit: PositiveInt,
    metrics: SynchronizerAlias => CommitmentMetrics,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends ReceivedAcsCommitmentMatcherFactory
    with NamedLogging {

  override def startMatcherPipeline(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      tickSignaller: TickSignaller,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[(KillSwitch, Future[Done])] = {
    val periodStore = periodStoreLookup(synchronizerId).getOrElse(
      ErrorUtil.invalidState("AcsCommitmentPeriodStore not initialized")
    )
    val digestStore = digestStoreLookup(synchronizerId).getOrElse(
      ErrorUtil.invalidState("AcsDigestStore not initialized")
    )
    val loggerFactoryWithSynchronizer =
      loggerFactory.append("synchronizer", synchronizerId.toString)

    for {
      watermark <- periodStore.watermark()
      checkpoint <- digestStore.latestCheckpointUpTo(
        Offset.MaxValue,
        AcsDigestStore.checkpointTickFilter,
      )
    } yield {
      val startingOffset = watermark.matching
      val matcher = new ReceivedAcsCommitmentMatcher(
        periodStore,
        stringInterning,
        metrics(synchronizerAlias),
        loggerFactoryWithSynchronizer,
        parallelProcessingLimit,
      )
      val signalSource =
        Source(checkpoint.map(_.offset).toList).concat(
          tickSignaller
            .readSignals((), s"ReceivedAcsCommitmentMatcher($synchronizerId)")
            .map(_.signal)
        )
      val graph = internalIndexService
        .acsUpdates(synchronizerId, startingOffset)
        .via(ReceivedAcsCommitmentMatcher.synchronizationFlow(signalSource))
        // The kill switch must sit behind the synchronization flow so that the kill switch's completion signal
        // does not get blocked by buffered elements in front of the gate.
        .viaMat(KillSwitches.single)(Keep.right)
        .via(matcher.pipeline)
        .toMat(Sink.ignore)(Keep.both)
      val (killSwitch, doneF) =
        PekkoUtil.runSupervised(graph, s"ReceivedAcsCommitmentMatcher-$synchronizerId")
      (killSwitch, doneF)
    }
  }

}
