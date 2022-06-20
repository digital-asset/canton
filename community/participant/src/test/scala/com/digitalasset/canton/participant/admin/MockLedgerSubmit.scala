// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.ledger.api.refinements.ApiTypes.WorkflowId
import com.daml.ledger.api.v1.commands.{Command => ScalaCommand}
import com.daml.ledger.api.v1.completion.Completion
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.ledger.api.client.{CommandSubmitterWithRetry, LedgerSubmit}
import com.digitalasset.canton.lifecycle.AsyncOrSyncCloseable
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{Future, Promise}

/** Mock for capturing a single submitted command.
  */
class MockLedgerSubmit(override val logger: TracedLogger) extends LedgerSubmit {
  private val lastCommandPromise = Promise[ScalaCommand]()
  val lastCommand: Future[ScalaCommand] = lastCommandPromise.future
  override val timeouts = DefaultProcessingTimeouts.testing

  override def submitCommand(
      command: Seq[ScalaCommand],
      commandId: Option[String] = None,
      workflowId: Option[WorkflowId] = None,
      deduplictionTime: Option[NonNegativeFiniteDuration] = None,
  )(implicit traceContext: TraceContext): Future[CommandSubmitterWithRetry.CommandResult] = {
    command.headOption.foreach(lastCommandPromise.trySuccess)
    Future.successful(CommandSubmitterWithRetry.Success(Completion()))
  }

  override def submitAsync(
      commands: Seq[ScalaCommand],
      commandId: Option[String],
      workflowId: Option[WorkflowId],
      deduplicationTime: Option[NonNegativeFiniteDuration] = None,
  )(implicit traceContext: TraceContext): Future[Unit] =
    throw new IllegalArgumentException("Method not defined")

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = List.empty
}
