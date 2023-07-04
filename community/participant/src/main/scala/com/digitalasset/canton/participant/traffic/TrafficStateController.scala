// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.traffic

import com.daml.error.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.SyncServiceError.TrafficControlErrorGroup
import com.digitalasset.canton.sequencing.protocol.SequencedEventTrafficState
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.{MemberTrafficStatus, TopUpEvent, TopUpQueue}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

/** Maintains the current traffic state up to date for a given domain.
  */
class TrafficStateController(
    val participant: ParticipantId,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  private val currentTrafficState =
    new AtomicReference[Option[(SequencedEventTrafficState, CantonTimestamp)]](None)
  private val topUpQueue = new TopUpQueue(List.empty)

  def addTopUp(topUp: TopUpEvent): Unit = topUpQueue.addOne(topUp)

  def updateState(newState: SequencedEventTrafficState, timestamp: CantonTimestamp)(implicit
      tc: TraceContext
  ): Unit = {
    logger.trace(s"Updating traffic control state with $newState")
    currentTrafficState.get() match {
      // if the new state is older or equal to the new one, don't update, but log it instead
      case Some(state) if timestamp <= state._2 =>
        logger.debug(
          s"Received a traffic state update with a timestamp ($timestamp) <= to the existing state ($state). It will be ignored."
        )
      case _ =>
        currentTrafficState.set(Some(newState -> timestamp))
    }
  }

  def getState: Future[Option[MemberTrafficStatus]] = Future.successful {
    currentTrafficState
      .get()
      .map { case (trafficState, ts) =>
        MemberTrafficStatus(
          participant,
          ts,
          trafficState,
          topUpQueue.pruneUntilAndGetAllTopUpsFor(ts),
        )
      }
  }
}

object TrafficStateController {

  object TrafficControlError extends TrafficControlErrorGroup {
    sealed trait TrafficControlError extends Product with Serializable with CantonError

    @Explanation(
      """This error indicates that no available domain with that id could be found, and therefore
       no traffic state could be retrieved."""
    )
    @Resolution("Ensure that the participant is connected to the domain with the provided id.")
    object DomainIdNotFound
        extends ErrorCode(
          id = "TRAFFIC_CONTROL_DOMAIN_ID_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(domainId: DomainId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "The domain id was not found"
          )
          with TrafficControlError
    }

    @Explanation(
      """This error indicates that the participant does not have a traffic state."""
    )
    @Resolution(
      """Ensure that the the participant is connected to a domain with traffic control enabled,
        and that it has received at least one event from the domain since its connection."""
    )
    object TrafficStateNotFound
        extends ErrorCode(
          id = "TRAFFIC_CONTROL_STATE_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      final case class Error()(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "Traffic state not found"
          )
          with TrafficControlError
    }
  }

}
