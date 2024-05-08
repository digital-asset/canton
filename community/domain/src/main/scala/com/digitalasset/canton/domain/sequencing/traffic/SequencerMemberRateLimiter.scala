// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  GroupRecipient,
  TrafficState,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.EventCostCalculator
import com.digitalasset.canton.traffic.EventCostCalculator.EventCostDetail
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}
import monocle.macros.syntax.lens.*

import scala.concurrent.{ExecutionContext, Future}

class SequencerMemberRateLimiter(
    member: Member,
    override val loggerFactory: NamedLoggerFactory,
    metrics: SequencerMetrics,
    eventCostCalculator: EventCostCalculator,
    futureSupervisor: FutureSupervisor,
    timeouts: ProcessingTimeout,
) extends NamedLogging {

  // queue to push traffic metrics updates in order
  private val metricsUpdateQueue =
    new SimpleExecutionQueue("metric-update-queue", futureSupervisor, timeouts, loggerFactory)

  /** Consume the traffic costs of the event from the sequencer members traffic state.
    *
    * NOTE: This method must be called in order of the sequencing timestamps.
    */
  def tryConsume(
      event: Batch[ClosedEnvelope],
      sequencingTimestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
      trafficState: TrafficState,
      groupToMembers: Map[GroupRecipient, Set[Member]],
      currentBalance: NonNegativeLong,
      metricsContextFn: () => MetricsContext,
  )(implicit
      tc: TraceContext,
      executionContext: ExecutionContext,
  ): (
    Either[SequencerRateLimitError, TrafficState],
  ) = Either
    .cond(
      sequencingTimestamp > trafficState.timestamp,
      (),
      SequencerRateLimitError.EventOutOfOrder(member, trafficState.timestamp, sequencingTimestamp),
    )
    .flatMap { _ =>
      logger.trace(
        s"Calculating cost of event for sender $member at sequencing timestamp $sequencingTimestamp"
      )

      val eventCostDetails: EventCostDetail = eventCostCalculator.computeEventCostWithDetails(
        event,
        trafficControlConfig.readVsWriteScalingFactor,
        groupToMembers,
      )
      val eventCost = eventCostDetails.computeCost(logger)

      val (newTrafficState, accepted) =
        updateTrafficState(
          sequencingTimestamp,
          trafficControlConfig,
          eventCost,
          trafficState,
          currentBalance,
          metricsContextFn,
        )

      logger.debug(s"Updated traffic status for $member: $newTrafficState")

      if (accepted)
        Right(newTrafficState)
      else {
        logger.debug(
          s"Event cost $eventCost for sender $member exceeds traffic limit. Cost details: $eventCostDetails"
        )
        Left(
          SequencerRateLimitError.AboveTrafficLimit(
            member = member,
            trafficCost = eventCost,
            newTrafficState,
          )
        )
      }
    }

  private[traffic] def updateTrafficState(
      sequencingTimestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
      eventCost: NonNegativeLong,
      trafficState: TrafficState,
      currentBalance: NonNegativeLong,
      metricsContextFn: () => MetricsContext,
  )(implicit tc: TraceContext, executionContext: ExecutionContext) = {
    // Get the traffic limit for this event and prune the in memory queue until then, since we won't need earlier top ups anymore
    require(
      currentBalance >= trafficState.extraTrafficConsumed,
      s"Extra traffic limit at $sequencingTimestamp (${currentBalance.value}) is < to extra traffic consumed (${trafficState.extraTrafficConsumed})",
    )
    val newExtraTrafficRemainder = currentBalance.value - trafficState.extraTrafficConsumed.value

    // determine the time elapsed since we approved last time
    val deltaMicros = sequencingTimestamp.toMicros - trafficState.timestamp.toMicros
    val trafficAllowedSinceLastTimestamp = Math
      .floor(trafficControlConfig.baseRate.value * deltaMicros.toDouble / 1e6)
      .toLong

    // determine the whole number of bytes that we were allowed to submit in that period
    // That is accumulated traffic from latest allowed update + whatever was left at the time,
    // upper-bounded by maxBurst
    val allowedByteFromBaseRate =
      Math.min(
        trafficControlConfig.maxBaseTrafficAmount.value,
        trafficAllowedSinceLastTimestamp + trafficState.baseTrafficRemainder.value,
      )

    if (allowedByteFromBaseRate == trafficControlConfig.maxBaseTrafficAmount.value)
      logger.trace(
        s"Member $member reached their max burst limit of $allowedByteFromBaseRate at $sequencingTimestamp"
      )

    // If we're below the allowed base rate limit, just consume from that
    val (newState, accepted) = if (eventCost.value <= allowedByteFromBaseRate) {
      trafficState
        .focus(_.extraTrafficRemainder)
        .replace(NonNegativeLong.tryCreate(newExtraTrafficRemainder))
        .focus(_.baseTrafficRemainder)
        .replace(NonNegativeLong.tryCreate(allowedByteFromBaseRate - eventCost.value))
        .focus(_.timestamp)
        .replace(sequencingTimestamp) -> true

      // Otherwise...
    } else {
      // The extra traffic needed is the event cost minus what we can use from base rate
      val extraTrafficConsumed =
        NonNegativeLong.tryCreate(eventCost.value - allowedByteFromBaseRate)
      // ...if that's below how much extra traffic we have, keep going
      if (extraTrafficConsumed.value <= newExtraTrafficRemainder) {

        trafficState
          // Set the base traffic remainder to zero since we need to use all of it
          .focus(_.baseTrafficRemainder)
          .replace(NonNegativeLong.zero)
          // Increase the extra traffic consumed
          .focus(_.extraTrafficConsumed)
          .modify(_ + extraTrafficConsumed)
          // Replace the old remainder with new remainder - extra traffic consumed
          .focus(_.extraTrafficRemainder)
          .replace(NonNegativeLong.tryCreate(newExtraTrafficRemainder - extraTrafficConsumed.value))
          .focus(_.timestamp)
          .replace(sequencingTimestamp) -> true

      } else {
        // If not, update the remaining base and extra traffic but deny the event
        trafficState
          .focus(_.extraTrafficRemainder)
          .replace(NonNegativeLong.tryCreate(newExtraTrafficRemainder))
          .focus(_.baseTrafficRemainder)
          .replace(NonNegativeLong.tryCreate(allowedByteFromBaseRate))
          .focus(_.timestamp)
          .replace(sequencingTimestamp) -> false
      }
    }

    FutureUtil.doNotAwaitUnlessShutdown(
      // We need to schedule updates sequentially because we update gauges with the absolute value of the traffic state
      // So updating out of order would give incorrect metric values
      metricsUpdateQueue.execute(
        Future {
          val memberMc = MetricsContext("member" -> member.toString)
          val requestSpecificMc = metricsContextFn().merge(memberMc)

          if (accepted)
            metrics.trafficControl.eventDelivered.mark(eventCost.value)(requestSpecificMc)
          else metrics.trafficControl.eventRejected.mark(eventCost.value)(requestSpecificMc)

          metrics.trafficControl
            .lastTrafficUpdateTimestamp(memberMc)
            .updateValue(newState.timestamp.getEpochSecond)

          newState.extraTrafficLimit
            .map[Long](_.value)
            .foreach(etl => metrics.trafficControl.extraTrafficLimit(memberMc).updateValue(etl))
          metrics.trafficControl
            .extraTrafficConsumed(memberMc)
            .updateValue(newState.extraTrafficConsumed.value)
          metrics.trafficControl
            .baseTrafficRemainder(memberMc)
            .updateValue(newState.baseTrafficRemainder.value)
        },
        s"Updating traffic metrics for $member",
      ),
      s"Failed to update traffic metrics for member $member",
    )

    (newState, accepted)
  }
}
