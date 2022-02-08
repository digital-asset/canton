// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer
import akka.NotUsed
import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.Source
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerMemberId
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.lifecycle.AsyncOrSyncCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Ignore local writes and simply trigger reads periodically based on a static polling interval.
  * Suitable for horizontally scaled sequencers where the local process will not have in-process visibility of all writes.
  */
class PollingEventSignaller(
    pollingInterval: NonNegativeFiniteDuration,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
) extends EventSignaller
    with NamedLogging {
  override def notifyOfLocalWrite(notification: WriteNotification)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    Future.unit

  override def readSignalsForMember(
      member: Member,
      memberId: SequencerMemberId,
  ): Source[ReadSignal, NotUsed] =
    Source
      .repeat(ReadSignal)
      .delay(pollingInterval.toScala, strategy = DelayOverflowStrategy.backpressure)

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq()
}
