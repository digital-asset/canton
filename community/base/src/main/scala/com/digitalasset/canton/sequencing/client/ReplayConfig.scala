// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.transports.replay.ReplayClient
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext

import java.nio.file.Path
import scala.concurrent.{Future, Promise}

/** Configuration for where to record sequencer sends and events to.
  * @param directory
  *   Root directory for holding all recording files
  * @param filename
  *   Filename that is initially empty and updated to a name based on the member-id at runtime. Use
  *   [[setFilename]] to ensure this can only be set once.
  */
final case class RecordingConfig(directory: Path, filename: Option[String] = None) {
  def setFilename(value: String): RecordingConfig = filename.fold(copy(filename = Some(value))) {
    existingFilename =>
      sys.error(s"Recording filename has already been set: $existingFilename")
  }

  /** Gets the full filepath and throws if the filepath has not yet been set. */
  lazy val fullFilePath: Path = filename.fold(sys.error("filename has not been set")) { filename =>
    directory.resolve(filename)
  }
}

/** Configuration for setting up a sequencer client to replay requests or received events.
  * @param recordingConfig
  *   The path to where all recorded content is stored
  * @param action
  *   What type of replay we'll be performing
  */
final case class ReplayConfig(recordingConfig: RecordingConfig, action: ReplayAction)

object ReplayConfig {
  def apply(recordingBasePath: Path, action: ReplayAction): ReplayConfig =
    ReplayConfig(RecordingConfig(recordingBasePath), action)
}

sealed trait ReplayAction

object ReplayAction {

  /** Replay events received from the sequencer */
  case object SequencerEvents extends ReplayAction

  /** Replay sends that were made to the sequencer. Tests can control the
    * [[transports.replay.ReplayClient]] once constructed by waiting for the `replayClient` future
    * to be completed with the `ReplayClient` instance.
    */
  final case class SequencerSends(
      override protected val loggerFactory: NamedLoggerFactory,
      sendTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.tryOfSeconds(20),
      private val replayClientP: Promise[ReplayClient] = Promise[ReplayClient](),
      usePekko: Boolean = false,
  ) extends ReplayAction
      with NamedLogging {

    private[client] def publishReplayClient(
        replayClient: ReplayClient
    ): Unit =
      if (replayClientP.trySuccess(replayClient)) {
        logger.info("Publishing replay client")(TraceContext.empty)
      }

    val replayClient: Future[ReplayClient] = replayClientP.future
  }
}
