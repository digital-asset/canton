// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.SubmissionRequestType
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ErrorUtil

/** Reordering events within a block
  *
  * In order to simplify and improve processing, we move all events that may impact the processing
  * of the block to the end of the block. This allows to process all events without having to worry
  * about computing some intermediate state as a result of the block updates.
  *
  * While we are at it, we also reorder verdicts, confirmation requests and responses to optimize
  * processing such that nodes prefer "finishing" things before "starting" things.
  *
  * Note that this method runs before the timestamps get fixed and before the injection of
  * post-ordering ticks.
  */
trait BlockReorderer {
  def reordered(events: Seq[Traced[LedgerBlockEvent]])(implicit
      traceContext: TraceContext
  ): Seq[Traced[LedgerBlockEvent]]
}

object BlockReorderer {
  lazy val NoOp: BlockReorderer = new BlockReorderer {
    override def reordered(events: Seq[Traced[LedgerBlockEvent]])(implicit
        traceContext: TraceContext
    ): Seq[Traced[LedgerBlockEvent]] =
      events
  }

  class Impl(consistencyChecks: Boolean, val loggerFactory: NamedLoggerFactory)
      extends BlockReorderer
      with NamedLogging {

    override def reordered(
        events: Seq[Traced[LedgerBlockEvent]]
    )(implicit traceContext: TraceContext): Seq[Traced[LedgerBlockEvent]] =
      if (events.isEmpty) events
      else {
        // we compute the minimum timestamp and decorate events with their priority (first tuple position) and index (last position)
        val (mapped, minTs) = events.view.zipWithIndex
          .foldLeft(
            (Vector.empty[(Int, Traced[LedgerBlockEvent], Int)], CantonTimestamp.MaxValue)
          ) { case ((acc, minTs), (event, idx)) =>
            val res = event.value match {
              case LedgerBlockEvent.Send(_, signedSubmissionRequest, _, _) =>
                (priorityOfEvent(signedSubmissionRequest), event, idx)

              case LedgerBlockEvent.Acknowledgment(_, _) =>
                (priorityOfAcknowledgment, event, idx)
            }
            (
              acc :+ res,
              minTs.min(event.value.timestamp),
            )
          }

        // we also order by index to preserve the original order of events with the same priority and timestamp
        val sorted = mapped.sortBy { case (priority, event, idx) =>
          (priority, event.value.timestamp, idx)
        }
        val tmp =
          sorted.foldLeft((minTs, Seq.empty[Traced[LedgerBlockEvent]])) {
            case ((nextTs, accumulatedEvents), (_, event, _)) =>
              (
                nextTs.immediateSuccessor,
                event.map(_.withTimestamp(nextTs)) +: accumulatedEvents,
              )
          }
        val result = tmp._2.reverse
        // the following conditions will hold: we are using the same min timestamp as the input events.
        // the output timestamp may be lower or equal to the max timestamp of the input events
        if (consistencyChecks) {
          ErrorUtil.requireState(
            result.map(_.value.timestamp).minOption == events.map(_.value.timestamp).minOption,
            "min values mismatch!",
          )
        }
        result
      }

    private def priorityOfEvent(request: SignedSubmissionRequest) =
      request.content.requestType match {
        case SubmissionRequestType.ConfirmationResponse => 1
        case SubmissionRequestType.ConfirmationRequest => 2
        case SubmissionRequestType.Verdict => 0
        case SubmissionRequestType.Commitment => 3
        case SubmissionRequestType.TopUp => 6
        case SubmissionRequestType.TopUpMed => 6
        case SubmissionRequestType.TopologyTransaction => 4
        case SubmissionRequestType.TimeProof => 5
        case SubmissionRequestType.Unexpected(err) =>
          noTracingLogger.warn(s"Unexpected submission request type\n  $err\n  $request")
          0
        case SubmissionRequestType.LsuSequencingTest => 5
      }
    private val priorityOfAcknowledgment: Int = 7

  }

}
