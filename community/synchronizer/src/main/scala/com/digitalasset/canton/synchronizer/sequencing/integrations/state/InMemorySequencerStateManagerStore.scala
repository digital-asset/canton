// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.integrations.state

import cats.syntax.functorFilter.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.AggregationId
import com.digitalasset.canton.synchronizer.block.update.InFlightAggregations
import com.digitalasset.canton.synchronizer.sequencer.{
  FreshInFlightAggregation,
  InFlightAggregation,
  InFlightAggregationUpdate,
  InFlightAggregationUpdates,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference

class InMemorySequencerStateManagerStore(
    protected val loggerFactory: NamedLoggerFactory
) extends SequencerStateManagerStore
    with NamedLogging {

  private val state: AtomicReference[State] =
    new AtomicReference[State](State.empty)

  override def readInFlightAggregations(
      timestamp: CantonTimestamp,
      maxSequencingTimeUpperBound: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[InFlightAggregations] = {
    val snapshot = state.get()
    val inFlightAggregations = snapshot.inFlightAggregations.byId
      .mapFilter(
        _.project(timestamp).filter(_.maxSequencingTimestamp <= maxSequencingTimeUpperBound)
      )
    FutureUnlessShutdown.pure(InFlightAggregations.fromMap(inFlightAggregations))
  }

  private case class MemberIndex(
      addedAt: CantonTimestamp,
      events: Seq[OrdinarySerializedEvent] = Seq.empty,
      lastAcknowledged: Option[CantonTimestamp] = None,
      isEnabled: Boolean = true,
  )

  private object State {
    val empty: State =
      State(
        indices = Map.empty,
        pruningLowerBound = None,
        maybeOnboardingTopologyTimestamp = None,
        inFlightAggregations = InFlightAggregations.empty,
      )
  }

  private case class State(
      indices: Map[Member, MemberIndex],
      pruningLowerBound: Option[CantonTimestamp],
      maybeOnboardingTopologyTimestamp: Option[CantonTimestamp],
      inFlightAggregations: InFlightAggregations,
  ) {
    def addInFlightAggregationUpdates(
        updates: InFlightAggregationUpdates
    )(implicit traceContext: TraceContext): State =
      this.copy(inFlightAggregations = updates.foldLeft(inFlightAggregations) {
        case (aggregations, (aggregationId, update)) =>
          aggregations.updatedWith(aggregationId) { previousO =>
            tryApplyUpdate(
              aggregationId,
              previousO,
              update,
            )
          }
      })

    private def tryApplyUpdate(
        aggregationId: AggregationId,
        inFlightAggregationO: Option[InFlightAggregation],
        update: InFlightAggregationUpdate,
    )(implicit loggingContext: ErrorLoggingContext): InFlightAggregation = {
      val InFlightAggregationUpdate(freshO, aggregatedSenders) = update
      val inFlightAggregation = inFlightAggregationO match {
        case None =>
          val fresh = freshO.getOrElse(
            ErrorUtil.internalError(
              new IllegalArgumentException(
                s"Missing in-flight aggregation information for ID $aggregationId"
              )
            )
          )
          InFlightAggregation.initial(fresh)
        case Some(inFlightAggregation) =>
          freshO.foreach { fresh =>
            val existing = FreshInFlightAggregation(
              inFlightAggregation.maxSequencingTimestamp,
              inFlightAggregation.rule,
            )
            ErrorUtil.requireArgument(
              fresh == existing,
              s"Mismatch with existing in-flight aggregation: existing: $existing, new: $fresh",
            )
          }
          inFlightAggregation
      }
      aggregatedSenders.foldLeft(inFlightAggregation)((inFlightAgg, senderAggregation) =>
        inFlightAgg
          .tryAggregate(senderAggregation)
          .getOrElse(inFlightAgg)
      )
    }

    def pruneExpiredInFlightAggregations(upToInclusive: CantonTimestamp): State =
      this.copy(inFlightAggregations = this.inFlightAggregations.cleanExpired(upToInclusive))
  }

  override def addInFlightAggregationUpdates(updates: InFlightAggregationUpdates)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    state.getAndUpdate(_.addInFlightAggregationUpdates(updates)).discard[State]
  }

  override def pruneExpiredInFlightAggregations(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    state
      .updateAndGet(_.pruneExpiredInFlightAggregations(upToInclusive))
      .inFlightAggregations
      .discard
  }
}
