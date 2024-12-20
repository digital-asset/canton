// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.ledger.participant.state.Update.SequencerIndexMoved
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{SequencerCounter, topology}

import scala.concurrent.ExecutionContext

// TODO(i21243): Clean up this and the trait as polymorphism is not needed here anymore
class ParticipantTopologyTerminateProcessingTicker(
    recordOrderPublisher: RecordOrderPublisher,
    domainId: DomainId,
    override protected val loggerFactory: NamedLoggerFactory,
) extends topology.processing.TerminateProcessing
    with NamedLogging {

  override def terminate(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.outcomeF(
      recordOrderPublisher.tick(
        SequencerIndexMoved(
          domainId = domainId,
          sequencerCounter = sc,
          recordTime = sequencedTime.value,
          requestCounterO = None,
        )
      )
    )
}
