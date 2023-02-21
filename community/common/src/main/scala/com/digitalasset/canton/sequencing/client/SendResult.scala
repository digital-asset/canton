// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.protocol.{Deliver, DeliverError, Envelope}
import com.digitalasset.canton.tracing.TraceContext

import scala.util.Try

/** Possible outcomes for a send operation can be observed by a SequencerClient */
sealed trait SendResult extends Product with Serializable

object SendResult {

  /** Send caused a deliver event to be successfully sequenced */
  case class Success(deliver: Deliver[Envelope[_]]) extends SendResult

  /** Send caused an event that indicates that the submission was not and never will be sequenced */
  sealed trait NotSequenced extends SendResult

  /** Send caused a deliver error to be sequenced */
  case class Error(error: DeliverError) extends NotSequenced

  /** No event was sequenced for the send up until the provided max sequencing time.
    * A correct sequencer implementation will no longer sequence any events from the send past this point.
    */
  case class Timeout(sequencerTime: CantonTimestamp) extends NotSequenced

  /** Log the value of this result to the given logger at an appropriate level and given description */
  def log(sendDescription: String, logger: TracedLogger)(
      result: UnlessShutdown[SendResult]
  )(implicit traceContext: TraceContext): Unit = result match {
    case UnlessShutdown.Outcome(SendResult.Success(deliver)) =>
      logger.trace(s"$sendDescription was sequenced at ${deliver.timestamp}")
    case UnlessShutdown.Outcome(SendResult.Error(error)) =>
      logger.warn(
        s"$sendDescription was rejected by the sequencer at ${error.timestamp} because [${error.reason}]"
      )
    case UnlessShutdown.Outcome(SendResult.Timeout(sequencerTime)) =>
      logger.warn(s"$sendDescription timed out at $sequencerTime")
    case UnlessShutdown.AbortedDueToShutdown =>
      logger.debug(s"$sendDescription aborted due to shutdown")
  }

  def toTry(sendDescription: String)(result: UnlessShutdown[SendResult]): Try[Unit] = result match {
    case UnlessShutdown.Outcome(SendResult.Success(_)) =>
      util.Success(())
    case UnlessShutdown.Outcome(SendResult.Error(error)) =>
      util.Failure(
        new RuntimeException(
          s"$sendDescription was rejected by the sequencer at ${error.timestamp} because [${error.reason}]"
        )
      )
    case UnlessShutdown.Outcome(SendResult.Timeout(sequencerTime)) =>
      util.Failure(new RuntimeException(s"$sendDescription timed out at $sequencerTime"))
    case UnlessShutdown.AbortedDueToShutdown =>
      util.Failure(new RuntimeException(s"$sendDescription was aborted due to shutdown"))
  }
}
