// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.TestVerdictSender.Result
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.{DefaultOpenEnvelope, MediatorRequest, Verdict}
import com.digitalasset.canton.sequencing.protocol.{AggregationRule, Batch}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

class TestVerdictSender extends VerdictSender {

  val sentResultsQueue: java.util.concurrent.BlockingQueue[Result] =
    new java.util.concurrent.LinkedBlockingQueue()

  def sentResults: Iterable[Result] = sentResultsQueue.asScala

  override def sendResult(
      requestId: RequestId,
      request: MediatorRequest,
      verdict: Verdict,
      decisionTime: CantonTimestamp,
      aggregationRule: Option[AggregationRule],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    sentResultsQueue.add(Result(requestId, decisionTime, Some(request), Some(verdict), None))
    Future.unit
  }

  override def sendResultBatch(
      requestId: RequestId,
      batch: Batch[DefaultOpenEnvelope],
      decisionTime: CantonTimestamp,
      aggregationRule: Option[AggregationRule],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    sentResultsQueue.add(Result(requestId, decisionTime, None, None, Some(batch)))
    Future.unit
  }
}

object TestVerdictSender {
  final case class Result(
      requestId: RequestId,
      decisionTime: CantonTimestamp,
      request: Option[MediatorRequest],
      verdict: Option[Verdict],
      batch: Option[Batch[DefaultOpenEnvelope]],
  )
}
