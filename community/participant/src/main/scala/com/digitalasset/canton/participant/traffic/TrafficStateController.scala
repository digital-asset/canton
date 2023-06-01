// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.traffic

import cats.instances.option.*
import cats.syntax.parallel.*
import com.daml.error.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveLong}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.v0.TrafficControlStateResponse
import com.digitalasset.canton.participant.sync.SyncServiceError.TrafficControlErrorGroup
import com.digitalasset.canton.participant.traffic.TrafficStateController.ParticipantTrafficState
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.{DomainId, Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Maintains the current traffic state up to date for a given domain.
  */
class TrafficStateController(
    topologyClient: DomainTopologyClientWithInit,
    participant: ParticipantId,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  private val currentTrafficState = new AtomicReference[Option[TrafficState]](None)

  def updateState(newState: TrafficState)(implicit tc: TraceContext): Unit = {
    logger.trace(s"Updating traffic control state with $newState")
    currentTrafficState.get().foreach { state =>
      require(
        newState.timestamp > state.timestamp,
        s"Expected new traffic state to have a strictly more recent timestamp than the old state. Old state at ${state.timestamp}. New state at ${newState.timestamp}",
      )
    }
    currentTrafficState.set(Some(newState))
  }

  def getState()(implicit
      tc: TraceContext,
      ec: ExecutionContext,
  ): Future[Option[ParticipantTrafficState]] = {
    currentTrafficState
      .get()
      .parTraverse { trafficState =>
        for {
          snapshot <- topologyClient.awaitSnapshot(trafficState.timestamp)
          trafficTopology <- snapshot.trafficControlStatus(Seq(participant))
        } yield {
          ParticipantTrafficState(
            trafficState.timestamp,
            trafficTopology
              .get(participant)
              .flatMap(_.map(_.totalExtraTrafficLimit)),
            trafficState.extraTrafficRemainder,
          )
        }
      }
  }
}

object TrafficStateController {
  final case class ParticipantTrafficState(
      timestamp: CantonTimestamp,
      totalExtraTrafficLimit: Option[PositiveLong],
      extraTrafficRemainder: NonNegativeLong,
  ) {
    def toProto: TrafficControlStateResponse.TrafficControlState = {
      TrafficControlStateResponse.TrafficControlState(
        Some(timestamp.toProtoPrimitive),
        totalExtraTrafficLimit = totalExtraTrafficLimit.map(_.value),
        extraTrafficRemainder = extraTrafficRemainder.value,
      )
    }
  }

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
      """This error indicates that the requested member is unknown."""
    )
    @Resolution("Ensure that the member provided is registered to the sequencer.")
    object MemberNotFound
        extends ErrorCode(
          id = "TRAFFIC_CONTROL_MEMBER_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(member: Member)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "The member was not found"
          )
          with TrafficControlError
    }
  }

}
