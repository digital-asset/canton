// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.sequencing.SequencerAggregatorXImpl.EventAndOrdinal
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.ApplicationHandlerFailure
import com.digitalasset.canton.sequencing.client.{DelaySequencedEvent, SequencedEventValidator}
import com.digitalasset.canton.sequencing.{SequencerAggregator, SequencerClientRecorder}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.util.SingleUseCell

import scala.concurrent.ExecutionContext

trait SubscriptionHandlerFactory {
  def create(
      eventValidator: SequencedEventValidator,
      initialPriorEventO: Option[EventAndOrdinal],
      sequencerAlias: SequencerAlias,
      sequencerId: SequencerId,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): SubscriptionHandler
}

class SubscriptionHandlerFactoryImpl(
    clock: Clock,
    metrics: SequencerClientMetrics,
    applicationHandlerFailure: SingleUseCell[ApplicationHandlerFailure],
    recorderO: Option[SequencerClientRecorder],
    sequencerAggregator: SequencerAggregator,
    processingDelay: DelaySequencedEvent,
    timeouts: ProcessingTimeout,
) extends SubscriptionHandlerFactory {

  override def create(
      eventValidator: SequencedEventValidator,
      initialPriorEventO: Option[EventAndOrdinal],
      sequencerAlias: SequencerAlias,
      sequencerId: SequencerId,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): SubscriptionHandler = new SubscriptionHandlerImpl(
    clock,
    metrics,
    applicationHandlerFailure,
    recorderO,
    sequencerAggregator,
    eventValidator,
    processingDelay,
    initialPriorEventO,
    sequencerAlias,
    sequencerId,
    timeouts,
    loggerFactory,
  )
}
