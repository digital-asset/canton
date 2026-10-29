// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.ledger.api.benchtool.util.ObserverWithResult
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future

object CompletionsObserver {
  def apply(endOffsetInclusive: Option[Long] = None): CompletionsObserver = new CompletionsObserver(
    endOffsetInclusive = endOffsetInclusive,
    logger = LoggerFactory.getLogger(getClass),
  )
}

class CompletionsObserver(endOffsetInclusive: Option[Long], logger: Logger)
    extends ObserverWithResult[CompletionStreamResponse, ObservedCompletions](logger) {

  private val completions = collection.mutable.ArrayBuffer[ObservedCompletion]()
  private val cancelled = new AtomicBoolean(false)

  override def streamName: String = "dummy-stream-name"

  override def onNext(value: CompletionStreamResponse): Unit = {
    for {
      completion <- value.completionResponse.completion
    } {
      completions.addOne(
        ObservedCompletion(
          userId = completion.userId,
          actAs = completion.actAs,
        )
      )
    }
    cancelIfEndOffsetReached(value)
  }

  private def cancelIfEndOffsetReached(value: CompletionStreamResponse): Unit =
    for {
      end <- endOffsetInclusive
      offset <- value.completionResponse.completion
        .map(_.offset)
        .orElse(value.completionResponse.offsetCheckpoint.map(_.offset))
      if offset >= end && cancelled.compareAndSet(false, true)
    } {
      logger.info(withStreamName(s"Reached end offset $end, cancelling the stream."))
      cancel()
    }

  override def completeWith(): Future[ObservedCompletions] = Future.successful(
    ObservedCompletions(
      completions = completions.toList
    )
  )
}

final case class ObservedCompletion(userId: String, actAs: Seq[String])

final case class ObservedCompletions(
    completions: Seq[ObservedCompletion]
)
