// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.SequencerAggregator.{
  MessageAggregationConfig,
  SequencerAggregatorError,
}
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.BlockingQueue
import scala.annotation.unused
import scala.concurrent.ExecutionContext

class SequencerAggregatorXImpl(
    @unused postAggregationHandler: PostAggregationHandler,
    @unused cryptoPureApi: CryptoPureApi,
    @unused eventInboxSize: PositiveInt,
    val loggerFactory: NamedLoggerFactory,
    @unused initialConfig: MessageAggregationConfig,
    @unused updateSendTracker: Seq[SequencedEventWithTraceContext[?]] => Unit,
    override val timeouts: ProcessingTimeout,
    @unused futureSupervisor: FutureSupervisor,
) extends SequencerAggregator {

  override def getLatestProcessedEventO: Option[SequencedSerializedEvent] = ???

  override def eventQueue: BlockingQueue[SequencedSerializedEvent] = ???

  override def changeMessageAggregationConfig(newConfig: MessageAggregationConfig): Unit = ???

  override def combine(
      messages: NonEmpty[Seq[SequencedSerializedEvent]]
  ): Either[SequencerAggregatorError, SequencedSerializedEvent] = ???

  override def combineAndMergeEvent(sequencerId: SequencerId, message: SequencedSerializedEvent)(
      implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Either[SequencerAggregatorError, Unit]] = ???
}
