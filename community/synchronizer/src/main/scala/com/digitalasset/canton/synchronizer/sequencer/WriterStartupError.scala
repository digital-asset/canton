// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

/** Errors that can occur while starting the [[SequencerWriter]]. Such errors, for example
  * discovering an unexpected commit mode, are not recoverable and the writer propagates the
  * failure.
  */
trait WriterStartupError

object WriterStartupError {

  /** We don't want to run the sequencer with a database commit mode that could lead to data loss,
    * so this is optionally and by default validated upon startup.
    */
  final case class BadCommitMode(message: String) extends WriterStartupError

  /** The sequencer is in the process of shutting down so cannot create a new sequencer writer. If
    * we happen to attempt to recreate the writer during a shutdown this will be logged at error
    * level but can be safely ignored.
    */
  case object WriterShuttingDown extends WriterStartupError

  /** Failed to reset the watermark during the recovery
    * @param message
    */
  final case class WatermarkResetError(message: String) extends WriterStartupError
}
