// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.messages.Verdict
import com.digitalasset.canton.protocol.{AlarmStreamer, RequestId, RootHash, ViewHash}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil._

private[mediator] sealed trait MediatorError extends Product with Serializable {
  def requestId: RequestId
}

private[mediator] case class StaleVersion(
    requestId: RequestId,
    newVersion: CantonTimestamp,
    staleVersion: CantonTimestamp,
) extends MediatorError

private[mediator] sealed trait ResponseAggregationError extends MediatorError {
  def alarm(sender: ParticipantId, alarmer: AlarmStreamer)(implicit
      traceContext: TraceContext
  ): Unit
}

private[mediator] case class MediatorRequestNotFound(
    requestId: RequestId,
    viewHashO: Option[ViewHash] = None,
    rootHashO: Option[RootHash] = None,
) extends ResponseAggregationError {
  override def alarm(sender: ParticipantId, alarmer: AlarmStreamer)(implicit
      traceContext: TraceContext
  ): Unit = {
    val viewHashMsg = viewHashO.fold("")(viewHash => show" for view hash $viewHash")
    val rootHashMsg = rootHashO.fold("")(rootHash => show" for root hash $rootHash")
    val _ = alarmer.alarm(
      s"unknown request ${requestId.unwrap}: received mediator response by $sender$viewHashMsg$rootHashMsg"
    )
  }
}

private[mediator] case class UnexpectedMediatorResponse(
    requestId: RequestId,
    viewHash: ViewHash,
    parties: Set[LfPartyId],
) extends ResponseAggregationError {
  override def alarm(sender: ParticipantId, alarmer: AlarmStreamer)(implicit
      traceContext: TraceContext
  ): Unit = {
    val _ =
      alarmer.alarm(
        s"request ${requestId.unwrap}: unexpected mediator response by $sender on behalf of $parties"
      )
  }
}

private[mediator] case class UnauthorizedMediatorResponse(
    requestId: RequestId,
    viewHash: ViewHash,
    sender: ParticipantId,
    parties: Set[LfPartyId],
) extends ResponseAggregationError {
  override def alarm(sender: ParticipantId, alarmer: AlarmStreamer)(implicit
      traceContext: TraceContext
  ): Unit = {
    val _ = alarmer.alarm(
      s"request ${requestId.unwrap}: unauthorized mediator response for view $viewHash by $sender on behalf of $parties"
    )
  }

}

private[mediator] case class MediatorRequestAlreadyFinalized(
    requestId: RequestId,
    existingVerdict: Verdict,
) extends ResponseAggregationError {
  override def alarm(sender: ParticipantId, alarmer: AlarmStreamer)(implicit
      traceContext: TraceContext
  ): Unit = ()
}
