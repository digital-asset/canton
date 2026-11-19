// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

/** Errors that can occur while starting the [[SequencerWriter]]. In the HA Sequencer Writer it is
  * possible due to racy assignment of writer locks that the startup may fail. This is by design and
  * starting the writer can simply be retried. However other errors such as discovering incorrect
  * configuration or a unexpected commit mode will not be recoverable and the writer should
  * propagate the failure.
  */
trait WriterStartupError {

  /** If true we can retry starting the [[SequencerWriter]]. Otherwise the error will be propagated.
    */
  val retryable: Boolean
}

object WriterStartupError {

  /** We don't want to run the sequencer with a database commit mode that could lead to data loss,
    * so this is optionally and by default validated upon startup.
    */
  final case class BadCommitMode(message: String) extends WriterStartupError {
    override val retryable =
      false // likely set at the database or user level, so retrying won't make a difference
  }

  /** The sequencer is in the process of shutting down so cannot create a new sequencer writer. If
    * we happen to attempt to recreate the writer during a shutdown this will be logged at error
    * level but can be safely ignored.
    */
  case object WriterShuttingDown extends WriterStartupError {
    override val retryable = false
  }

  /** Failed to reset the watermark during the recovery
    * @param message
    */
  final case class WatermarkResetError(message: String) extends WriterStartupError {
    // only possible via concurrent modification, which is only possible via misconfiguration,
    // as in unified sequencer there should only be one writer
    override val retryable = false
  }
}
