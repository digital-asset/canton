// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SubmissionRequestAmplification
import com.digitalasset.canton.sequencing.client.SequencerClient.{
  SequencerTransportContainer,
  SequencerTransports,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Mutex

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.Success

class SequencersTransportState(
    initialSequencerTransports: SequencerTransports[?],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with FlagCloseable {

  private val closeReasonPromise = Promise[SequencerClient.CloseReason]()

  private val lock = Mutex()

  private val states = new mutable.HashMap[SequencerId, SequencerTransportState]()

  private val sequencerTrustThreshold =
    new AtomicReference[PositiveInt](initialSequencerTransports.sequencerTrustThreshold)

  private val submissionRequestAmplification =
    new AtomicReference[SubmissionRequestAmplification](
      initialSequencerTransports.submissionRequestAmplification
    )

  def getSubmissionRequestAmplification: SubmissionRequestAmplification =
    submissionRequestAmplification.get()

  initialSequencerTransports.sequencerIdToTransportMapO.foreach { sequencerIdToTransportMap =>
    lock.exclusive {
      val sequencerIdToTransportStateMap =
        sequencerIdToTransportMap.map { case (sequencerId, transport) =>
          (sequencerId, SequencerTransportState(transport))
        }
      states.addAll(sequencerIdToTransportStateMap).discard
    }
  }

  def changeTransport(
      sequencerTransports: SequencerTransports[?]
  ): Unit =
    lock.exclusive {
      sequencerTrustThreshold.set(sequencerTransports.sequencerTrustThreshold)
      submissionRequestAmplification.set(sequencerTransports.submissionRequestAmplification)
    }

  private def closeSubscription(
      sequencerId: SequencerId,
      sequencerState: SequencerTransportState,
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Closing sequencer subscription $sequencerId...")
    sequencerState.transport.clientTransport.close()
  }

  def closeAllSubscriptions()(implicit traceContext: TraceContext): Unit = {
    val connections = lock.exclusive {
      val copy = states.toSeq
      states.clear()
      copy
    }
    // stop connections in parallel
    connections
      .foreach { case (sequencerId, subscription) => closeSubscription(sequencerId, subscription) }
    closeReasonPromise
      .tryComplete(Success(SequencerClient.CloseReason.ClientShutdown))
      .discard

  }

  override protected def onClosed(): Unit = TraceContext.withNewTraceContext("closing")(
    closeAllSubscriptions()(_)
  )
}

final case class SequencerTransportState(transport: SequencerTransportContainer[?])
