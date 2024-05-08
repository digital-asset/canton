// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  GroupRecipient,
  MemberRecipient,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.EventCostCalculator.{
  EventCostDetail,
  EventCostDetailByEnvelope,
  EventCostDetailByEnvelopeImpl,
}
import com.digitalasset.canton.util.ErrorUtil
import com.google.common.annotations.VisibleForTesting

object EventCostCalculator {

  /** Detailed cost information for each envelope in an event.
    */
  final case class EventCostDetail(envelopesCost: Seq[EventCostDetailByEnvelope])
      extends PrettyPrinting {
    def computeCost(logger: TracedLogger)(implicit traceContext: TraceContext): NonNegativeLong = {
      envelopesCost.map(_.computeCost(logger)).foldLeft(NonNegativeLong.zero)(_ + _)
    }

    override def pretty: Pretty[EventCostDetail] = prettyOfClass(
      param("envelopesCost", _.envelopesCost)
    )
  }

  trait EventCostDetailByEnvelope extends PrettyPrinting {
    def computeCost(logger: TracedLogger)(implicit traceContext: TraceContext): NonNegativeLong
  }

  /** Detailed cost information for one envelope.
    */
  final case class EventCostDetailByEnvelopeImpl(
      envelope: ClosedEnvelope,
      writeCosts: Long,
      recipientsSize: Int,
      costMultiplier: PositiveInt,
  ) extends EventCostDetailByEnvelope {
    def computeCost(logger: TracedLogger)(implicit traceContext: TraceContext): NonNegativeLong = {
      // read costs are based on the write costs and multiplied by the number of recipients with a readVsWrite cost multiplier
      try {
        // `writeCosts` and `recipientsSize` are originally Int, so multiplying them together cannot overflow a long
        val readCosts =
          math.multiplyExact(
            writeCosts * recipientsSize.toLong,
            costMultiplier.value.toLong,
          ) / 10000

        NonNegativeLong
          .create(math.addExact(readCosts, writeCosts))
          .valueOr { e =>
            ErrorUtil
              .invalidState(
                s"Traffic cost computation resulted in a negative value. Please report this as a bug. writeCosts = $writeCosts, recipientSize = $recipientsSize, costMultiplier = $costMultiplier: $e"
              )(ErrorLoggingContext.fromTracedLogger(logger))
          }

      } catch {
        case _: ArithmeticException =>
          throw new IllegalStateException(
            s"""Overflow in cost computation:
               |  writeCosts = $writeCosts
               |  recipientsSize = $recipientsSize
               |  costMultiplier = $costMultiplier""".stripMargin
          )
      }
    }

    override def pretty: Pretty[EventCostDetailByEnvelopeImpl] = prettyOfClass(
      param("envelope", _.envelope),
      param("writeCosts", _.writeCosts),
      param("recipientsSize", _.recipientsSize),
      param("costMultiplier", _.costMultiplier),
    )
  }
}

// TODO(i12907): Precise costs calculations
class EventCostCalculator(override val loggerFactory: NamedLoggerFactory) extends NamedLogging {
  def computeEventCostWithDetails(
      event: Batch[ClosedEnvelope],
      costMultiplier: PositiveInt,
      groupToMembers: Map[GroupRecipient, Set[Member]],
  ): EventCostDetail = {
    EventCostDetail(
      event.envelopes.map(computeEnvelopeCostWithDetails(costMultiplier, groupToMembers))
    )
  }

  @VisibleForTesting
  protected def payloadSize(envelope: ClosedEnvelope): Int = envelope.bytes.size()

  @VisibleForTesting
  def computeEnvelopeCostWithDetails(
      costMultiplier: PositiveInt,
      groupToMembers: Map[GroupRecipient, Set[Member]],
  )(envelope: ClosedEnvelope): EventCostDetailByEnvelope = {
    val writeCosts = payloadSize(envelope).toLong

    val allRecipients = envelope.recipients.allRecipients.toSeq
    val recipientsSize = allRecipients.map {
      case recipient: GroupRecipient => groupToMembers.get(recipient).map(_.size).getOrElse(0)
      case _: MemberRecipient => 1
    }.sum

    EventCostDetailByEnvelopeImpl(envelope, writeCosts, recipientsSize, costMultiplier)
  }
}
